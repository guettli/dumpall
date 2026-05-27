package main

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strings"

	"github.com/gavv/cobradoc"
	"github.com/spf13/pflag"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/cluster-api/cmd/clusterctl/cmd"
	"sigs.k8s.io/kustomize/kyaml/kio"
	yaml "sigs.k8s.io/yaml/goyaml.v3"
)

const (
	clusterNamespace       = "_cluster"
	redactionMarkerValue   = "REDCATED-BY-DUMPALL"
	toolNameForUsageOutput = "dumpall"
	lastAppliedAnnotation  = "kubectl.kubernetes.io/last-applied-configuration"
)

//go:embed common-ignore-config.yaml
var commonIgnoreConfig []byte

var toSkip = map[string][]string{
	"apps/v1": {"replicasets"},
	"authentication.k8s.io/v1": {
		"selfsubjectreviews", "tokenreviews",
	},
	"authorization.k8s.io/v1": {
		"selfsubjectaccessreviews", "subjectaccessreviews",
		"selfsubjectrulesreviews", "localsubjectaccessreviews",
	},
	"coordination.k8s.io/v1": {"leases"},
	"discovery.k8s.io/v1":    {"endpointslices"},
	"events.k8s.io/v1":       {"events"},
	"v1":                     {"events", "bindings", "componentstatuses", "endpoints"},
}

type options struct {
	outputDir             string
	quiet                 bool
	dumpSecrets           bool
	dumpManagedFields     bool
	removeOutdir          bool
	skipOwned             bool
	ignoreConfigUseCommon bool
	ignoreConfigFile      string
	readYamlFrom          string
	readResourceNamesFrom string
	namespacesCSV         string
	nameRegex             string
	skipNameGlob          string
	excludeNamespacesCSV  string
	comment               string
	fileName              string
	dir                   string
	namespaceFilter       map[string]struct{}
	nameFilterEnabled     bool
	nameFilterRegex       *regexp.Regexp
	ignoreRules           []ignoreRule
	skipRules             []skipRule
	excludeNamespaces     []string
	excludeFieldSelector  string
	resourceNameFilter    map[resourceNameKey]struct{}
}

type resourceNameKey struct {
	Kind      string
	Namespace string
	Name      string
}

type ignoreConfig struct {
	Rules             []ignoreRuleFile `yaml:"removeFields"`
	Skip              []skipRuleFile   `yaml:"skipResources"`
	ExcludeNamespaces []string         `yaml:"excludeNamespaces"`
}

type skipRuleFile struct {
	Group     *string `yaml:"group"`
	Kind      *string `yaml:"kind"`
	Namespace *string `yaml:"namespace"`
	Name      *string `yaml:"name"`
}

type skipRule struct {
	GroupPattern     string
	KindPattern      string
	NamespacePattern string
	NamePattern      string
}

type ignoreRuleFile struct {
	Group     *string               `yaml:"group"`
	Kind      *string               `yaml:"kind"`
	Namespace *string               `yaml:"namespace"`
	Name      *string               `yaml:"name"`
	Fields    []ignoreFieldEntryFile `yaml:"fields"`
}

// ignoreFieldEntryFile is a single entry in the fields list of a removeFields rule.
// It is either a plain field path string, or a map with "path" and optional "value" or
// "omitempty" keys. "omitempty" and "value" are mutually exclusive.
type ignoreFieldEntryFile struct {
	Path             string
	Value            any
	valueConstrained bool
	// OmitEmpty removes the field only when its value is nil, an empty map, an empty
	// slice, or an empty string — mirroring the json:",omitempty" / kubebuilder
	// +optional convention. Mutually exclusive with a value constraint.
	OmitEmpty bool
}

func (e *ignoreFieldEntryFile) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		e.Path = value.Value
		return nil
	case yaml.MappingNode:
		hasValue, hasOmitEmpty := false, false
		for i := 0; i < len(value.Content)-1; i += 2 {
			key := value.Content[i].Value
			switch key {
			case "path":
			case "value":
				hasValue = true
			case "omitempty":
				hasOmitEmpty = true
			default:
				return fmt.Errorf("unknown field %q in field entry (allowed: path, value, omitempty)", key)
			}
		}
		if hasValue && hasOmitEmpty {
			return fmt.Errorf("field entry cannot set both 'value' and 'omitempty'")
		}
		var m struct {
			Path      string `yaml:"path"`
			Value     any    `yaml:"value"`
			OmitEmpty bool   `yaml:"omitempty"`
		}
		if err := value.Decode(&m); err != nil {
			return err
		}
		if m.Path == "" {
			return fmt.Errorf("field entry map must have a non-empty 'path' key")
		}
		e.Path = m.Path
		if hasValue {
			e.Value = m.Value
			e.valueConstrained = true
		}
		e.OmitEmpty = m.OmitEmpty
		return nil
	default:
		return fmt.Errorf("field entry must be a string or a map with 'path', 'value', or 'omitempty' keys")
	}
}

type ignoreRule struct {
	GroupPattern     string
	KindPattern      string
	NamespacePattern string
	NamePattern      string
	Fields           []ignoreFieldPath
}

type ignoreFieldPath struct {
	segments         []string
	value            any
	valueConstrained bool
	omitEmpty        bool
}

type objectIdentity struct {
	Group     string
	Kind      string
	Namespace string
	Name      string
}

type resourceWriteJob struct {
	item         *unstructured.Unstructured
	isNamespaced bool
	sourceName   string
}

type gvrListJob struct {
	groupVersion string
	resourceName string
	gvr          schema.GroupVersionResource
	isNamespaced bool
}

type processingEvent struct {
	writtenFile string
	logMessage  string
	err         error
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "gendocs" {
		b := &bytes.Buffer{}

		err := cobradoc.WriteDocument(b, cmd.RootCmd, cobradoc.Markdown, cobradoc.Options{})
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		usageFile := "usage.md"

		err = os.WriteFile(usageFile, b.Bytes(), 0o600)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Printf("Created %q\n", usageFile)
		os.Exit(0)
	}

	if len(os.Args) > 1 && os.Args[1] == "show-common-ignore-config" {
		if len(os.Args) > 2 {
			fmt.Printf("show-common-ignore-config does not accept arguments: %v\n", os.Args[2:])
			os.Exit(1)
		}

		err := writeCommonIgnoreConfig(os.Stdout)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		os.Exit(0)
	}

	err := mainWithError()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func mainWithError() error {
	opts := &options{}
	pflag.StringVarP(&opts.outputDir, "out-dir", "o", "out", "Output directory (must not exist)")
	pflag.BoolVarP(&opts.quiet, "quiet", "q", false, "Quiet, suppress output")
	pflag.BoolVarP(&opts.dumpSecrets, "dump-secrets", "s", false, "Dump secrets (disabled by default)")
	pflag.BoolVarP(&opts.dumpManagedFields, "dump-managed-fields", "m", false, "Dump managed fields (disabled by default)")
	pflag.BoolVarP(&opts.removeOutdir, "remove-out-dir", "r", false, "Remove out-dir before dumping (disabled by default)")
	pflag.BoolVarP(&opts.skipOwned, "skip-owned", "O", false, "Skip resources that have a controlling owner reference (e.g., Pods owned by a ReplicaSet) or that Kubernetes autogenerates from other resources (e.g., aggregated ClusterRoles)")
	pflag.BoolVar(&opts.ignoreConfigUseCommon, "ignore-config-use-common", false, "Use the embedded common ignore config")
	pflag.StringVar(&opts.ignoreConfigFile, "ignore-config", "", "Path to a YAML file with ignore rules")
	pflag.StringVarP(&opts.namespacesCSV, "namespaces", "n", "", "Comma-separated list of namespaces to dump")
	pflag.StringVarP(&opts.nameRegex, "name-regex", "x", "", "Only dump resources where metadata.name matches this regex")
	pflag.StringVar(&opts.skipNameGlob, "skip-name-glob", "", "Skip resources where metadata.name matches this glob (e.g. 'foo-*' skips names starting with 'foo-')")
	pflag.StringVar(&opts.excludeNamespacesCSV, "exclude-namespaces", "", "Comma-separated list of namespace globs to fully exclude (e.g. 'foo-*,test-*'). Drops the Namespace object plus all resources inside; uses fieldSelector to skip those namespaces api-server-side")
	pflag.StringVar(&opts.readYamlFrom, "read-yaml-from", "", "Read YAML manifests from a file or directory instead of connecting to the api-server. Useful for normalizing existing YAML files for better diffing.")
	pflag.StringVar(&opts.readResourceNamesFrom, "read-resource-names-from", "", "Read resource identifiers (kind/namespace/name) from a YAML file or directory and dump only those resources. Useful to dump a specific subset of cluster resources.")
	pflag.StringVar(&opts.comment, "comment", "", "Additional comment line to add at the top of each output YAML file")
	pflag.StringVarP(&opts.fileName, "file-name", "f", "", "Alias for --read-yaml-from (hidden)")
	_ = pflag.CommandLine.MarkHidden("file-name")
	pflag.StringVar(&opts.dir, "dir", "", "Alias for --read-yaml-from (hidden)")
	_ = pflag.CommandLine.MarkHidden("dir")

	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s\nRead resources from the api-server (or a YAML file/directory via --read-yaml-from) and dump each resource to a file.\n\nSubcommands:\n  show-common-ignore-config   Print the embedded common ignore config\n", toolNameForUsageOutput)
		pflag.PrintDefaults()
	}

	pflag.Parse()

	if len(pflag.Args()) > 0 {
		pflag.Usage()
		return fmt.Errorf("unexpected positional arguments: %v", pflag.Args())
	}

	namespaceFilter, err := parseNamespaceFilter(opts.namespacesCSV)
	if err != nil {
		return err
	}

	opts.namespaceFilter = namespaceFilter

	nameFilterRegex, nameFilterEnabled, err := parseNameFilterRegex(opts.nameRegex)
	if err != nil {
		return err
	}

	opts.nameFilterEnabled = nameFilterEnabled
	opts.nameFilterRegex = nameFilterRegex

	opts.readYamlFrom = strings.TrimSpace(opts.readYamlFrom)
	opts.dir = strings.TrimSpace(opts.dir)
	if opts.dir != "" {
		if opts.readYamlFrom != "" {
			return fmt.Errorf("--dir and --read-yaml-from are mutually exclusive")
		}
		opts.readYamlFrom = opts.dir
	}
	opts.fileName = strings.TrimSpace(opts.fileName)
	if opts.fileName != "" {
		if opts.readYamlFrom != "" {
			return fmt.Errorf("--file-name (-f) and --read-yaml-from are mutually exclusive")
		}
		opts.readYamlFrom = opts.fileName
	}
	opts.readResourceNamesFrom = strings.TrimSpace(opts.readResourceNamesFrom)

	ignoreRules, skipRules, excludes, err := loadIgnoreRules(opts.ignoreConfigUseCommon, opts.ignoreConfigFile)
	if err != nil {
		return err
	}

	opts.ignoreRules = ignoreRules
	opts.skipRules = skipRules
	opts.excludeNamespaces = excludes

	if strings.TrimSpace(opts.skipNameGlob) != "" {
		pattern := opts.skipNameGlob
		flagRule, err := compileSkipRule(skipRuleFile{Name: &pattern})
		if err != nil {
			return fmt.Errorf("invalid --skip-name-glob: %w", err)
		}

		opts.skipRules = append(opts.skipRules, flagRule)
	}

	cliExcludes, err := parseExcludeNamespacesCSV(opts.excludeNamespacesCSV)
	if err != nil {
		return err
	}

	opts.excludeNamespaces = append(opts.excludeNamespaces, cliExcludes...)

	if opts.removeOutdir {
		err := os.RemoveAll(opts.outputDir)
		if err != nil {
			return fmt.Errorf("failed to remove out-dir %s: %w", opts.outputDir, err)
		}
	}

	_, err = os.Stat(opts.outputDir)
	if err == nil {
		return fmt.Errorf("output directory %q already exists. Use --remove-out-dir if you want to overwrite it", opts.outputDir)
	}

	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to inspect output directory %q: %w", opts.outputDir, err)
	}

	if opts.readResourceNamesFrom != "" {
		filter, err := loadResourceNameFilter(opts.readResourceNamesFrom)
		if err != nil {
			return err
		}
		opts.resourceNameFilter = filter
	}

	if opts.readYamlFrom != "" {
		info, err := os.Stat(opts.readYamlFrom)
		if err != nil {
			return fmt.Errorf("failed to inspect --read-yaml-from path %q: %w", opts.readYamlFrom, err)
		}
		if info.IsDir() {
			return readYamlFromDir(opts.readYamlFrom, opts)
		}
		return readYamlFromFile(opts.readYamlFrom, opts)
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)

	config, err := kubeconfig.ClientConfig()
	if err != nil {
		return fmt.Errorf("failed to get client config: %w", err)
	}

	// Disable client-side throttling. Modern api-servers can handle this,
	// and dumpall already bounds concurrency with worker pools.
	config.QPS = -1
	config.Burst = -1

	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %w", err)
	}

	resourceList, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		return fmt.Errorf("failed to discover resources: %w", err)
	}

	return readFromAPIServer(dynClient, resourceList, opts)
}

func readYamlFromFile(fileName string, opts *options) error {
	writeJobs, events, writerDone, writerCount := startWriteWorkers(opts)
	producerDone := make(chan struct{}, 1)

	go func() {
		defer func() {
			producerDone <- struct{}{}
		}()

		bytes, err := os.ReadFile(fileName)
		if err != nil {
			events <- processingEvent{
				err: fmt.Errorf("failed to read file %s: %w", fileName, err),
			}
			return
		}

		err = enqueueYAMLBytesAsJobs(bytes, fileName, opts, writeJobs)
		if err != nil {
			events <- processingEvent{err: err}
		}
	}()

	closeEventsWhenDone(producerDone, 1, writerDone, writerCount, writeJobs, events)

	return finalizeProcessing(events, opts)
}

func readYamlFromDir(dirPath string, opts *options) error {
	yamlFiles, err := findYAMLFiles(dirPath)
	if err != nil {
		return err
	}

	writeJobs, events, writerDone, writerCount := startWriteWorkers(opts)

	fileWorkerCount := boundedWorkerCount(len(yamlFiles))
	fileJobs := make(chan string, fileWorkerCount*2)
	producerDone := make(chan struct{}, fileWorkerCount)

	for range fileWorkerCount {
		go func() {
			defer func() {
				producerDone <- struct{}{}
			}()

			for filePath := range fileJobs {
				bytes, err := os.ReadFile(filePath)
				if err != nil {
					events <- processingEvent{
						err: fmt.Errorf("failed to read file %s: %w", filePath, err),
					}
					continue
				}

				err = enqueueYAMLBytesAsJobs(bytes, filePath, opts, writeJobs)
				if err != nil {
					events <- processingEvent{err: err}
				}
			}
		}()
	}

	go func() {
		for _, filePath := range yamlFiles {
			fileJobs <- filePath
		}
		close(fileJobs)
	}()

	closeEventsWhenDone(producerDone, fileWorkerCount, writerDone, writerCount, writeJobs, events)

	return finalizeProcessing(events, opts)
}

func findYAMLFiles(dirPath string) ([]string, error) {
	info, err := os.Stat(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect directory %s: %w", dirPath, err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("input path %q is not a directory", dirPath)
	}

	yamlFiles := make([]string, 0)
	err = filepath.WalkDir(dirPath, func(currentPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			return nil
		}

		if !isYAMLFile(currentPath) {
			return nil
		}

		yamlFiles = append(yamlFiles, currentPath)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk directory %s: %w", dirPath, err)
	}

	sort.Strings(yamlFiles)

	return yamlFiles, nil
}

func loadResourceNameFilter(fromPath string) (map[resourceNameKey]struct{}, error) {
	var filePaths []string

	info, err := os.Stat(fromPath)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect --read-resource-names-from path %q: %w", fromPath, err)
	}

	if info.IsDir() {
		filePaths, err = findYAMLFiles(fromPath)
		if err != nil {
			return nil, err
		}
	} else {
		filePaths = []string{fromPath}
	}

	filter := make(map[resourceNameKey]struct{})
	for _, filePath := range filePaths {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
		}

		nodes, err := kio.FromBytes(data)
		if err != nil {
			return nil, fmt.Errorf("failed to parse YAML in %s: %w", filePath, err)
		}

		for _, node := range nodes {
			m, err := node.Map()
			if err != nil {
				return nil, fmt.Errorf("failed to convert document in %s to map: %w", filePath, err)
			}

			u := &unstructured.Unstructured{Object: m}
			ns := u.GetNamespace()
			if ns == "" {
				ns = clusterNamespace
			}

			filter[resourceNameKey{Kind: u.GetKind(), Namespace: ns, Name: u.GetName()}] = struct{}{}
		}
	}

	return filter, nil
}

func isYAMLFile(filePath string) bool {
	switch strings.ToLower(filepath.Ext(filePath)) {
	case ".yaml", ".yml":
		return true
	default:
		return false
	}
}

func enqueueYAMLBytesAsJobs(bytes []byte, sourceName string, opts *options, jobs chan<- resourceWriteJob) error {
	nodes, err := kio.FromBytes(bytes)
	if err != nil {
		return fmt.Errorf("failed to parse YAML in %s: %w", sourceName, err)
	}

	for i := range nodes {
		node := nodes[i]

		m, err := node.Map()
		if err != nil {
			return fmt.Errorf("failed to convert document %d in %s to map: %w", i+1, sourceName, err)
		}

		u := &unstructured.Unstructured{Object: m}

		ns, _, err := unstructured.NestedString(m, "metadata", "namespace")
		if err != nil {
			return fmt.Errorf("failed to get namespace for item %s in %s: %w", u.GetName(), sourceName, err)
		}

		isNamespaced := ns != ""

		if !shouldProcessItem(u, isNamespaced, opts) {
			continue
		}

		jobs <- resourceWriteJob{
			item:         u,
			isNamespaced: isNamespaced,
			sourceName:   sourceName,
		}
	}

	return nil
}

func validateNamespaceFilter(client dynamic.Interface, opts *options) error {
	if len(opts.namespaceFilter) == 0 {
		return nil
	}

	list, err := client.Resource(namespacesGVR).List(context.TODO(), meta.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list namespaces for --namespaces validation: %w", err)
	}

	existing := make(map[string]struct{}, len(list.Items))
	for i := range list.Items {
		existing[list.Items[i].GetName()] = struct{}{}
	}

	missing := make([]string, 0)
	for ns := range opts.namespaceFilter {
		if _, ok := existing[ns]; !ok {
			missing = append(missing, ns)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("--namespaces: namespace(s) not found in cluster: %s", strings.Join(missing, ", "))
	}

	return nil
}

func readFromAPIServer(client dynamic.Interface, resourceList []*meta.APIResourceList, opts *options) error {
	if err := resolveExcludeNamespaceFieldSelector(client, opts); err != nil {
		return err
	}

	if err := validateNamespaceFilter(client, opts); err != nil {
		return err
	}

	sort.Slice(resourceList, func(i int, j int) bool {
		return resourceList[i].GroupVersion < resourceList[j].GroupVersion
	})

	gvrJobs := make([]gvrListJob, 0)
	for _, apiGroup := range resourceList {
		sort.Slice(apiGroup.APIResources, func(i int, j int) bool {
			return apiGroup.APIResources[i].Name < apiGroup.APIResources[j].Name
		})

		for _, resource := range apiGroup.APIResources {
			skipSlice := toSkip[apiGroup.GroupVersion]
			if slices.Contains(skipSlice, resource.Name) {
				continue
			}

			group := getGroup(apiGroup.GroupVersion)
			if skipRuleCoversGVR(opts.skipRules, group, resource.Kind) {
				continue
			}

			gvrJobs = append(gvrJobs, gvrListJob{
				groupVersion: apiGroup.GroupVersion,
				resourceName: resource.Name,
				gvr: schema.GroupVersionResource{
					Group:    group,
					Version:  getVersion(apiGroup.GroupVersion),
					Resource: resource.Name,
				},
				isNamespaced: resource.Namespaced,
			})
		}
	}

	writeJobs, events, writerDone, writerCount := startWriteWorkers(opts)
	listJobs := make(chan gvrListJob, boundedWorkerCount(len(gvrJobs))*2)
	listerWorkerCount := boundedWorkerCount(len(gvrJobs))
	producerDone := make(chan struct{}, listerWorkerCount)

	for range listerWorkerCount {
		go func() {
			defer func() {
				producerDone <- struct{}{}
			}()

			for job := range listJobs {
				err := enqueueGVRItems(client, job, opts, writeJobs)
				if err != nil {
					events <- processingEvent{
						logMessage: fmt.Sprintf("Failed to process resource %q %s: %v", job.groupVersion, job.resourceName, err),
					}
				}
			}
		}()
	}

	go func() {
		for _, job := range gvrJobs {
			listJobs <- job
		}
		close(listJobs)
	}()

	closeEventsWhenDone(producerDone, listerWorkerCount, writerDone, writerCount, writeJobs, events)

	return finalizeProcessing(events, opts)
}

var namespacesGVR = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}

// resolveExcludeNamespaceFieldSelector resolves the configured globs against
// the live namespace list and stores a fieldSelector on opts so namespaced
// listings skip those namespaces api-server-side.
func resolveExcludeNamespaceFieldSelector(client dynamic.Interface, opts *options) error {
	if len(opts.excludeNamespaces) == 0 {
		return nil
	}

	list, err := client.Resource(namespacesGVR).List(context.TODO(), meta.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list namespaces for --exclude-namespaces resolution: %w", err)
	}

	excluded := make([]string, 0)
	for i := range list.Items {
		name := list.Items[i].GetName()
		for _, pattern := range opts.excludeNamespaces {
			if matchGlob(pattern, name) {
				excluded = append(excluded, name)
				break
			}
		}
	}

	if len(excluded) == 0 {
		return nil
	}

	parts := make([]string, 0, len(excluded))
	for _, name := range excluded {
		parts = append(parts, "metadata.namespace!="+name)
	}

	opts.excludeFieldSelector = strings.Join(parts, ",")
	return nil
}

func enqueueGVRItems(client dynamic.Interface, job gvrListJob, opts *options, writeJobs chan<- resourceWriteJob) error {
	listOpts := meta.ListOptions{}
	if job.isNamespaced && opts.excludeFieldSelector != "" {
		listOpts.FieldSelector = opts.excludeFieldSelector
	}

	list, err := client.Resource(job.gvr).List(context.TODO(), listOpts)
	if err != nil {
		return fmt.Errorf("failed to list resources for %s: %w", job.gvr.Resource, err)
	}

	for i := range list.Items {
		item := list.Items[i]
		if !shouldProcessItem(&item, job.isNamespaced, opts) {
			continue
		}

		writeJobs <- resourceWriteJob{
			item:         &item,
			isNamespaced: job.isNamespaced,
			sourceName:   fmt.Sprintf("%s %s", job.groupVersion, job.resourceName),
		}
	}

	return nil
}

func startWriteWorkers(opts *options) (chan resourceWriteJob, chan processingEvent, chan struct{}, int) {
	writerCount := defaultWorkerCount()
	writeJobs := make(chan resourceWriteJob, writerCount*2)
	events := make(chan processingEvent, writerCount*2)
	writerDone := make(chan struct{}, writerCount)

	for range writerCount {
		go func() {
			defer func() {
				writerDone <- struct{}{}
			}()

			for job := range writeJobs {
				filePath, err := processUnstructured(job.item, job.isNamespaced, opts)
				if err != nil {
					events <- processingEvent{
						err: fmt.Errorf("failed to process item %s from %s: %w", job.item.GetName(), job.sourceName, err),
					}
					continue
				}

				events <- processingEvent{
					writtenFile: filePath,
				}
			}
		}()
	}

	return writeJobs, events, writerDone, writerCount
}

func closeEventsWhenDone(producerDone <-chan struct{}, producerCount int, writerDone <-chan struct{}, writerCount int, writeJobs chan resourceWriteJob, events chan processingEvent) {
	go func() {
		for range producerCount {
			<-producerDone
		}

		close(writeJobs)

		for range writerCount {
			<-writerDone
		}

		close(events)
	}()
}

func finalizeProcessing(events <-chan processingEvent, opts *options) error {
	fileCount, err := consumeProcessingEvents(events, opts)
	if err == nil && !opts.quiet {
		fmt.Printf("Total files written: %d\n", fileCount)
	}

	return err
}

func consumeProcessingEvents(events <-chan processingEvent, opts *options) (int64, error) {
	var (
		fileCount int64
		firstErr  error
	)

	for event := range events {
		switch {
		case event.err != nil:
			if firstErr == nil {
				firstErr = event.err
			}
		case event.logMessage != "":
			fmt.Println(event.logMessage)
		case event.writtenFile != "":
			fileCount++
			if !opts.quiet {
				fmt.Printf("Written: %s\n", event.writtenFile)
			}
		}
	}

	return fileCount, firstErr
}

func defaultWorkerCount() int {
	return boundedWorkerCount(runtime.GOMAXPROCS(0))
}

func boundedWorkerCount(jobCount int) int {
	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 2 {
		workerCount = 2
	}

	if jobCount > 0 && workerCount > jobCount {
		return jobCount
	}

	return workerCount
}

func processUnstructured(item *unstructured.Unstructured, isNamespaced bool, opts *options) (string, error) {
	ns := item.GetNamespace()
	if !isNamespaced {
		ns = clusterNamespace
	}

	gvk := item.GroupVersionKind()
	name := item.GetName()

	var dirPath string
	if gvk.Group == "" {
		dirPath = filepath.Join(opts.outputDir, ns, gvk.Kind)
	} else {
		dirPath = filepath.Join(opts.outputDir, ns, fmt.Sprintf("%s_%s", gvk.Group, gvk.Kind))
	}

	err := os.MkdirAll(dirPath, 0o755)
	if err != nil {
		return "", fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}

	filePath := filepath.Join(dirPath, fmt.Sprintf("%s.yaml", sanitizePath(name)))

	err = writeYAML(filePath, item.Object, opts)
	if err != nil {
		return "", err
	}

	return filePath, nil
}

func writeYAML(filePath string, obj map[string]any, opts *options) error {
	metadata, ok := obj["metadata"].(map[string]any)
	if !ok {
		return fmt.Errorf("metadata not found in object")
	}

	if !opts.dumpManagedFields {
		delete(metadata, "managedFields")
	}

	if !opts.dumpSecrets {
		redactSecretValues(obj)
	}

	applyIgnoreRules(obj, opts)
	pruneEmptyMetadataMaps(obj)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer file.Close()

	fmt.Fprintf(file, "# https://github.com/guettli/dumpall version: %s\n", getBuildVersion())
	if opts.comment != "" {
		fmt.Fprintf(file, "# %s\n", opts.comment)
	}

	encoder := yaml.NewEncoder(file)
	defer encoder.Close()

	encoder.SetIndent(2)

	err = encoder.Encode(obj)
	if err != nil {
		return fmt.Errorf("failed to write YAML to file %s: %w", filePath, err)
	}

	return nil
}

func getBuildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	return info.Main.Version
}

func pruneEmptyMetadataMaps(obj map[string]any) {
	metadata, ok := obj["metadata"].(map[string]any)
	if !ok {
		return
	}

	pruneEmptyMetadataMap(metadata, "annotations")
	pruneEmptyMetadataMap(metadata, "labels")
}

func pruneEmptyMetadataMap(metadata map[string]any, field string) {
	switch typed := metadata[field].(type) {
	case map[string]any:
		if len(typed) == 0 {
			delete(metadata, field)
		}
	case map[string]string:
		if len(typed) == 0 {
			delete(metadata, field)
		}
	}
}

func writeCommonIgnoreConfig(w io.Writer) error {
	_, err := w.Write(commonIgnoreConfig)
	if err != nil {
		return fmt.Errorf("failed to write embedded common ignore config: %w", err)
	}

	return nil
}

func loadIgnoreRules(useCommon bool, userConfigFile string) ([]ignoreRule, []skipRule, []string, error) {
	trimmedFileName := strings.TrimSpace(userConfigFile)
	rules := make([]ignoreRule, 0)
	skips := make([]skipRule, 0)
	excludes := make([]string, 0)

	if useCommon {
		commonRules, commonSkips, commonExcludes, err := parseIgnoreConfigBytes("embedded common ignore config", commonIgnoreConfig)
		if err != nil {
			return nil, nil, nil, err
		}

		rules = append(rules, commonRules...)
		skips = append(skips, commonSkips...)
		excludes = append(excludes, commonExcludes...)
	}

	if trimmedFileName == "" {
		return rules, skips, excludes, nil
	}

	userConfigBytes, err := os.ReadFile(trimmedFileName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read ignore config %s: %w", trimmedFileName, err)
	}

	userRules, userSkips, userExcludes, err := parseIgnoreConfigBytes(trimmedFileName, userConfigBytes)
	if err != nil {
		return nil, nil, nil, err
	}

	rules = append(rules, userRules...)
	skips = append(skips, userSkips...)
	excludes = append(excludes, userExcludes...)

	return rules, skips, excludes, nil
}

func parseIgnoreConfigBytes(name string, data []byte) ([]ignoreRule, []skipRule, []string, error) {
	if err := rejectLegacyIgnoreConfigKeys(name, data); err != nil {
		return nil, nil, nil, err
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)

	var cfg ignoreConfig
	if err := decoder.Decode(&cfg); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse ignore config %s: %w", name, err)
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, nil, nil, fmt.Errorf("failed to parse ignore config %s: expected a single YAML document", name)
		}

		return nil, nil, nil, fmt.Errorf("failed to parse ignore config %s: %w", name, err)
	}

	rules := make([]ignoreRule, 0, len(cfg.Rules))
	for idx := range cfg.Rules {
		rule, err := compileIgnoreRule(cfg.Rules[idx])
		if err != nil {
			return nil, nil, nil, fmt.Errorf("invalid ignore rule %d in %s: %w", idx+1, name, err)
		}

		rules = append(rules, rule)
	}

	skips := make([]skipRule, 0, len(cfg.Skip))
	for idx := range cfg.Skip {
		skip, err := compileSkipRule(cfg.Skip[idx])
		if err != nil {
			return nil, nil, nil, fmt.Errorf("invalid skip rule %d in %s: %w", idx+1, name, err)
		}

		skips = append(skips, skip)
	}

	excludes, err := validateExcludeNamespacePatterns(name, cfg.ExcludeNamespaces)
	if err != nil {
		return nil, nil, nil, err
	}

	return rules, skips, excludes, nil
}

func parseExcludeNamespacesCSV(csv string) ([]string, error) {
	trimmed := strings.TrimSpace(csv)
	if trimmed == "" {
		return nil, nil
	}

	patterns := make([]string, 0)
	for _, entry := range strings.Split(csv, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		patterns = append(patterns, entry)
	}

	if len(patterns) == 0 {
		return nil, fmt.Errorf("invalid --exclude-namespaces %q: no patterns found", csv)
	}

	return validateExcludeNamespacePatterns("--exclude-namespaces", patterns)
}

func validateExcludeNamespacePatterns(source string, patterns []string) ([]string, error) {
	out := make([]string, 0, len(patterns))
	for idx, pattern := range patterns {
		trimmed := strings.TrimSpace(pattern)
		if trimmed == "" {
			return nil, fmt.Errorf("invalid excludeNamespaces entry %d in %s: empty pattern", idx+1, source)
		}

		if err := validateGlobPattern(trimmed); err != nil {
			return nil, fmt.Errorf("invalid excludeNamespaces entry %d in %s: invalid glob %q: %w", idx+1, source, trimmed, err)
		}

		out = append(out, trimmed)
	}

	return out, nil
}

func compileIgnoreRule(fileRule ignoreRuleFile) (ignoreRule, error) {
	fields := make([]ignoreFieldPath, 0, len(fileRule.Fields))
	for _, field := range fileRule.Fields {
		trimmed := strings.TrimSpace(field.Path)
		if trimmed == "" {
			return ignoreRule{}, fmt.Errorf("fields must not contain empty entries")
		}

		segments, err := parseIgnoreFieldPath(trimmed)
		if err != nil {
			return ignoreRule{}, fmt.Errorf("invalid field path %q: %w", trimmed, err)
		}

		for _, segment := range segments {
			if segment == "..." || !hasFieldSegmentWildcard(segment) {
				continue
			}

			if err := validateGlobPattern(segment); err != nil {
				return ignoreRule{}, fmt.Errorf("invalid field path %q: invalid segment glob %q: %w", trimmed, segment, err)
			}
		}

		fields = append(fields, ignoreFieldPath{
			segments:         segments,
			value:            field.Value,
			valueConstrained: field.valueConstrained,
			omitEmpty:        field.OmitEmpty,
		})
	}

	if len(fields) == 0 {
		return ignoreRule{}, fmt.Errorf("fields must not be empty")
	}

	groupPattern := patternOrWildcard(fileRule.Group)
	if err := validateGlobPattern(groupPattern); err != nil {
		return ignoreRule{}, fmt.Errorf("invalid group glob %q: %w", groupPattern, err)
	}

	kindPattern := patternOrWildcard(fileRule.Kind)
	if err := validateGlobPattern(kindPattern); err != nil {
		return ignoreRule{}, fmt.Errorf("invalid kind glob %q: %w", kindPattern, err)
	}

	namespacePattern := patternOrWildcard(fileRule.Namespace)
	if err := validateGlobPattern(namespacePattern); err != nil {
		return ignoreRule{}, fmt.Errorf("invalid namespace glob %q: %w", namespacePattern, err)
	}

	namePattern := patternOrWildcard(fileRule.Name)
	if err := validateGlobPattern(namePattern); err != nil {
		return ignoreRule{}, fmt.Errorf("invalid name glob %q: %w", namePattern, err)
	}

	return ignoreRule{
		GroupPattern:     groupPattern,
		KindPattern:      kindPattern,
		NamespacePattern: namespacePattern,
		NamePattern:      namePattern,
		Fields:           fields,
	}, nil
}

// rejectLegacyIgnoreConfigKeys catches configs that still use the old
// top-level keys (`rules:` / `skip:`) and reports a clear rename hint instead
// of the generic "field not found in type" error from strict parsing.
func rejectLegacyIgnoreConfigKeys(name string, data []byte) error {
	var preview map[string]any
	if err := yaml.Unmarshal(data, &preview); err != nil {
		// Let the strict parser surface the real error.
		return nil
	}

	legacy := []struct {
		oldKey string
		newKey string
	}{
		{"rules", "removeFields"},
		{"skip", "skipResources"},
	}

	for _, entry := range legacy {
		if _, ok := preview[entry.oldKey]; ok {
			return fmt.Errorf("ignore config %s uses legacy key %q — rename it to %q", name, entry.oldKey, entry.newKey)
		}
	}

	return nil
}

func compileSkipRule(fileRule skipRuleFile) (skipRule, error) {
	if !skipRuleHasMatcher(fileRule) {
		return skipRule{}, fmt.Errorf("skip rule must set at least one of group, kind, namespace, name")
	}

	groupPattern := patternOrWildcard(fileRule.Group)
	if err := validateGlobPattern(groupPattern); err != nil {
		return skipRule{}, fmt.Errorf("invalid group glob %q: %w", groupPattern, err)
	}

	kindPattern := patternOrWildcard(fileRule.Kind)
	if err := validateGlobPattern(kindPattern); err != nil {
		return skipRule{}, fmt.Errorf("invalid kind glob %q: %w", kindPattern, err)
	}

	namespacePattern := patternOrWildcard(fileRule.Namespace)
	if err := validateGlobPattern(namespacePattern); err != nil {
		return skipRule{}, fmt.Errorf("invalid namespace glob %q: %w", namespacePattern, err)
	}

	namePattern := patternOrWildcard(fileRule.Name)
	if err := validateGlobPattern(namePattern); err != nil {
		return skipRule{}, fmt.Errorf("invalid name glob %q: %w", namePattern, err)
	}

	return skipRule{
		GroupPattern:     groupPattern,
		KindPattern:      kindPattern,
		NamespacePattern: namespacePattern,
		NamePattern:      namePattern,
	}, nil
}

func skipRuleHasMatcher(fileRule skipRuleFile) bool {
	for _, value := range []*string{fileRule.Group, fileRule.Kind, fileRule.Namespace, fileRule.Name} {
		if value != nil && strings.TrimSpace(*value) != "" {
			return true
		}
	}

	return false
}

func (r skipRule) matches(identity objectIdentity) bool {
	return matchGlob(r.GroupPattern, identity.Group) &&
		matchGlob(r.KindPattern, identity.Kind) &&
		matchGlob(r.NamespacePattern, identity.Namespace) &&
		matchGlob(r.NamePattern, identity.Name)
}

// skipRuleCoversGVR reports whether any rule would drop every resource of the
// given group+kind regardless of namespace or name. When this holds, the LIST
// call for that GVR can be skipped entirely — both as an optimization and to
// avoid noisy "forbidden" warnings on clusters where the caller lacks list
// permission for a kind the user already asked to skip.
func skipRuleCoversGVR(rules []skipRule, group string, kind string) bool {
	for _, rule := range rules {
		if rule.NamespacePattern != "*" || rule.NamePattern != "*" {
			continue
		}

		if matchGlob(rule.GroupPattern, group) && matchGlob(rule.KindPattern, kind) {
			return true
		}
	}

	return false
}

func patternOrWildcard(value *string) string {
	if value == nil {
		return "*"
	}

	return strings.TrimSpace(*value)
}

func validateGlobPattern(pattern string) error {
	_, err := path.Match(pattern, "")
	return err
}

func parseIgnoreFieldPath(fieldPath string) ([]string, error) {
	var segments []string
	var current strings.Builder
	escaped := false

	for i := 0; i < len(fieldPath); i++ {
		r := fieldPath[i]

		switch {
		case escaped:
			current.WriteByte(r)
			escaped = false
		case r == '\\':
			escaped = true
		case fieldPath[i:] == "..." || strings.HasPrefix(fieldPath[i:], "..."):
			if current.Len() > 0 {
				segments = append(segments, current.String())
				current.Reset()
			}

			segments = append(segments, "...")
			i += 2
		case r == '.':
			if current.Len() == 0 {
				return nil, fmt.Errorf("empty path segment")
			}

			segments = append(segments, current.String())
			current.Reset()
		default:
			current.WriteByte(r)
		}
	}

	if escaped {
		return nil, fmt.Errorf("path ends with an unfinished escape")
	}

	if current.Len() == 0 {
		if len(segments) > 0 && segments[len(segments)-1] == "..." {
			return nil, fmt.Errorf("path must not end with ...")
		}

		return nil, fmt.Errorf("empty path segment")
	}

	segments = append(segments, current.String())

	return segments, nil
}

func applyIgnoreRules(obj map[string]any, opts *options) {
	if len(opts.ignoreRules) == 0 {
		return
	}

	identity := objectIdentityFromObject(obj)
	for _, rule := range opts.ignoreRules {
		if !rule.matches(identity) {
			continue
		}

		for _, field := range rule.Fields {
			if opts.dumpManagedFields && isManagedFieldsPath(field.segments) {
				continue
			}

			deleteFieldPath(obj, field.segments, field.value, field.valueConstrained, field.omitEmpty)
		}
	}
}

func objectIdentityFromObject(obj map[string]any) objectIdentity {
	u := &unstructured.Unstructured{Object: obj}
	namespace := u.GetNamespace()
	if namespace == "" {
		namespace = clusterNamespace
	}

	return objectIdentity{
		Group:     getGroup(u.GetAPIVersion()),
		Kind:      u.GetKind(),
		Namespace: namespace,
		Name:      u.GetName(),
	}
}

func objectIdentityFromUnstructured(item *unstructured.Unstructured, isNamespaced bool) objectIdentity {
	namespace := item.GetNamespace()
	if !isNamespaced {
		namespace = clusterNamespace
	}

	return objectIdentity{
		Group:     getGroup(item.GetAPIVersion()),
		Kind:      item.GetKind(),
		Namespace: namespace,
		Name:      item.GetName(),
	}
}

func (r ignoreRule) matches(identity objectIdentity) bool {
	return matchGlob(r.GroupPattern, identity.Group) &&
		matchGlob(r.KindPattern, identity.Kind) &&
		matchGlob(r.NamespacePattern, identity.Namespace) &&
		matchGlob(r.NamePattern, identity.Name)
}

func matchGlob(pattern string, value string) bool {
	matched, err := path.Match(pattern, value)
	if err != nil {
		return false
	}

	return matched
}

func isManagedFieldsPath(segments []string) bool {
	return len(segments) == 2 && segments[0] == "metadata" && segments[1] == "managedFields"
}

func deleteFieldPath(current any, segments []string, constraintValue any, constrained bool, omitEmpty bool) {
	if len(segments) == 0 {
		return
	}

	if segments[0] == "..." {
		deleteFieldPathRecursive(current, segments[1:], constraintValue, constrained, omitEmpty)
		return
	}

	switch typed := current.(type) {
	case map[string]any:
		deleteFieldPathFromMap(typed, segments, constraintValue, constrained, omitEmpty)
	case map[string]string:
		deleteFieldPathFromStringMap(typed, segments, constraintValue, constrained, omitEmpty)
	case []any:
		for _, entry := range typed {
			deleteFieldPath(entry, segments, constraintValue, constrained, omitEmpty)
		}
	case []map[string]any:
		for _, entry := range typed {
			deleteFieldPath(entry, segments, constraintValue, constrained, omitEmpty)
		}
	case []map[string]string:
		for _, entry := range typed {
			deleteFieldPath(entry, segments, constraintValue, constrained, omitEmpty)
		}
	}
}

func deleteFieldPathRecursive(current any, remaining []string, constraintValue any, constrained bool, omitEmpty bool) {
	if len(remaining) == 0 {
		return
	}

	deleteFieldPath(current, remaining, constraintValue, constrained, omitEmpty)

	switch typed := current.(type) {
	case map[string]any:
		for _, value := range typed {
			deleteFieldPathRecursive(value, remaining, constraintValue, constrained, omitEmpty)
		}
	case []any:
		for _, entry := range typed {
			deleteFieldPathRecursive(entry, remaining, constraintValue, constrained, omitEmpty)
		}
	case []map[string]any:
		for _, entry := range typed {
			deleteFieldPathRecursive(entry, remaining, constraintValue, constrained, omitEmpty)
		}
	case []map[string]string:
		for _, entry := range typed {
			deleteFieldPathRecursive(entry, remaining, constraintValue, constrained, omitEmpty)
		}
	}
}

func deleteFieldPathFromMap(current map[string]any, segments []string, constraintValue any, constrained bool, omitEmpty bool) {
	keyPattern := segments[0]
	if !hasFieldSegmentWildcard(keyPattern) {
		if len(segments) == 1 {
			if constrained && !valueMatches(current[keyPattern], constraintValue) {
				return
			}
			if omitEmpty && !isEmpty(current[keyPattern]) {
				return
			}
			delete(current, keyPattern)
			return
		}

		next, ok := current[keyPattern]
		if !ok {
			return
		}

		deleteFieldPath(next, segments[1:], constraintValue, constrained, omitEmpty)
		return
	}

	for key, next := range current {
		if !matchGlob(keyPattern, key) {
			continue
		}

		if len(segments) == 1 {
			if constrained && !valueMatches(current[key], constraintValue) {
				continue
			}
			if omitEmpty && !isEmpty(current[key]) {
				continue
			}
			delete(current, key)
			continue
		}

		deleteFieldPath(next, segments[1:], constraintValue, constrained, omitEmpty)
	}
}

func deleteFieldPathFromStringMap(current map[string]string, segments []string, constraintValue any, constrained bool, omitEmpty bool) {
	if len(segments) != 1 {
		return
	}

	keyPattern := segments[0]
	if !hasFieldSegmentWildcard(keyPattern) {
		if constrained && !valueMatches(current[keyPattern], constraintValue) {
			return
		}
		if omitEmpty && !isEmpty(current[keyPattern]) {
			return
		}
		delete(current, keyPattern)
		return
	}

	for key := range current {
		if matchGlob(keyPattern, key) {
			if constrained && !valueMatches(current[key], constraintValue) {
				continue
			}
			if omitEmpty && !isEmpty(current[key]) {
				continue
			}
			delete(current, key)
		}
	}
}

func valueMatches(actual, expected any) bool {
	if fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected) {
		return true
	}
	return false
}

// isEmpty reports whether v is absent or empty, mirroring json:",omitempty" semantics:
// nil, empty map, empty slice, or empty string are all considered empty.
func isEmpty(v any) bool {
	if v == nil {
		return true
	}
	switch typed := v.(type) {
	case string:
		return typed == ""
	case map[string]any:
		return len(typed) == 0
	case map[string]string:
		return len(typed) == 0
	case []any:
		return len(typed) == 0
	default:
		return false
	}
}

func hasFieldSegmentWildcard(segment string) bool {
	return strings.ContainsAny(segment, "*?")
}

func getGroup(groupVersion string) string {
	parts := strings.Split(groupVersion, "/")
	if len(parts) > 1 {
		return parts[0]
	}

	return ""
}

func getVersion(groupVersion string) string {
	parts := strings.Split(groupVersion, "/")
	return parts[len(parts)-1]
}

func parseNamespaceFilter(namespacesCSV string) (map[string]struct{}, error) {
	filter := make(map[string]struct{})

	trimmed := strings.TrimSpace(namespacesCSV)
	if trimmed == "" {
		return filter, nil
	}

	for _, namespace := range strings.Split(namespacesCSV, ",") {
		namespace = strings.TrimSpace(namespace)
		if namespace == "" {
			continue
		}

		filter[namespace] = struct{}{}
	}

	if len(filter) == 0 {
		return nil, fmt.Errorf("invalid --namespaces %q: no namespace names found", namespacesCSV)
	}

	return filter, nil
}

func parseNameFilterRegex(nameRegex string) (*regexp.Regexp, bool, error) {
	trimmed := strings.TrimSpace(nameRegex)
	if trimmed == "" {
		return regexp.MustCompile(""), false, nil
	}

	re, err := regexp.Compile(trimmed)
	if err != nil {
		return nil, false, fmt.Errorf("invalid --name-regex %q: %w", nameRegex, err)
	}

	return re, true, nil
}

// matchesExcludedNamespace reports whether the item should be dropped because
// it lives in (or is) one of the excluded namespaces. Cluster-scoped Namespace
// objects are matched by their name; every other resource is matched by its
// metadata.namespace.
func matchesExcludedNamespace(item *unstructured.Unstructured, isNamespaced bool, patterns []string) bool {
	if len(patterns) == 0 {
		return false
	}

	target := ""
	switch {
	case isNamespaced:
		target = item.GetNamespace()
	case item.GetKind() == "Namespace" && getGroup(item.GetAPIVersion()) == "":
		target = item.GetName()
	}

	if target == "" {
		return false
	}

	for _, pattern := range patterns {
		if matchGlob(pattern, target) {
			return true
		}
	}

	return false
}

func hasControllingOwner(item *unstructured.Unstructured) bool {
	for _, ref := range item.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			return true
		}
	}

	return false
}

// isAutogeneratedByKubernetes reports whether the item is one that Kubernetes
// itself creates and maintains, rather than something a user authored. Such
// resources have no controlling owner reference but are still effectively
// "owned" by the control plane.
func isAutogeneratedByKubernetes(item *unstructured.Unstructured) bool {
	return isAggregatedClusterRole(item) ||
		isRBACBootstrapResource(item) ||
		isDefaultServiceAccount(item) ||
		isKubeRootCAConfigMap(item)
}

// isAggregatedClusterRole reports whether the item is a ClusterRole whose
// `rules` are autogenerated by the Kubernetes RBAC controller from other
// ClusterRoles via `aggregationRule.clusterRoleSelectors`.
func isAggregatedClusterRole(item *unstructured.Unstructured) bool {
	gvk := item.GroupVersionKind()
	if gvk.Group != "rbac.authorization.k8s.io" || gvk.Kind != "ClusterRole" {
		return false
	}

	rule, found, err := unstructured.NestedMap(item.Object, "aggregationRule")
	if err != nil || !found {
		return false
	}

	return len(rule) > 0
}

// isRBACBootstrapResource reports whether the item is a default RBAC resource
// installed by the Kubernetes bootstrap process. Those resources carry the
// label `kubernetes.io/bootstrapping=rbac-defaults` and are reconciled by the
// apiserver on every start, so user edits get overwritten.
func isRBACBootstrapResource(item *unstructured.Unstructured) bool {
	if item.GroupVersionKind().Group != "rbac.authorization.k8s.io" {
		return false
	}

	return item.GetLabels()["kubernetes.io/bootstrapping"] == "rbac-defaults"
}

// isDefaultServiceAccount reports whether the item is the `default`
// ServiceAccount that the service-account controller creates in every
// namespace.
func isDefaultServiceAccount(item *unstructured.Unstructured) bool {
	gvk := item.GroupVersionKind()
	return gvk.Group == "" && gvk.Kind == "ServiceAccount" && item.GetName() == "default"
}

// isKubeRootCAConfigMap reports whether the item is the `kube-root-ca.crt`
// ConfigMap that the root-ca-cert-publisher controller creates in every
// namespace.
func isKubeRootCAConfigMap(item *unstructured.Unstructured) bool {
	gvk := item.GroupVersionKind()
	return gvk.Group == "" && gvk.Kind == "ConfigMap" && item.GetName() == "kube-root-ca.crt"
}

func shouldProcessItem(item *unstructured.Unstructured, isNamespaced bool, opts *options) bool {
	if opts.resourceNameFilter != nil {
		ns := item.GetNamespace()
		if !isNamespaced || ns == "" {
			ns = clusterNamespace
		}
		key := resourceNameKey{Kind: item.GetKind(), Namespace: ns, Name: item.GetName()}
		if _, ok := opts.resourceNameFilter[key]; !ok {
			return false
		}
	}

	if opts.skipOwned && (hasControllingOwner(item) || isAutogeneratedByKubernetes(item)) {
		return false
	}

	if matchesExcludedNamespace(item, isNamespaced, opts.excludeNamespaces) {
		return false
	}

	if len(opts.skipRules) > 0 {
		identity := objectIdentityFromUnstructured(item, isNamespaced)
		for _, rule := range opts.skipRules {
			if rule.matches(identity) {
				return false
			}
		}
	}

	if opts.nameFilterEnabled {
		if !opts.nameFilterRegex.MatchString(item.GetName()) {
			return false
		}
	}

	if len(opts.namespaceFilter) == 0 {
		return true
	}

	if isNamespaced {
		_, ok := opts.namespaceFilter[item.GetNamespace()]
		return ok
	}

	if item.GetKind() == "Namespace" {
		_, ok := opts.namespaceFilter[item.GetName()]
		return ok
	}

	return false
}

var sanitizePathRegex = regexp.MustCompile(`[\\/:*?"'<>|!@#$%^&()+={}\[\];,]`)

func sanitizePath(path string) string {
	return sanitizePathRegex.ReplaceAllString(path, "_")
}

func redactSecretValues(obj map[string]any) {
	if !isCoreV1Secret(obj) {
		return
	}

	redactSecretField(obj, "data")
	redactSecretField(obj, "stringData")
	redactSecretAnnotations(obj)
}

var secretAPIGroupRegex = regexp.MustCompile(`^v\d+`)

func isCoreV1Secret(obj map[string]any) bool {
	kind, _ := obj["kind"].(string)
	if kind != "Secret" {
		return false
	}

	apiVersion, _ := obj["apiVersion"].(string)
	if apiVersion == "" {
		return true
	}

	if secretAPIGroupRegex.MatchString(apiVersion) {
		return true
	}

	return false
}

func redactSecretField(obj map[string]any, field string) {
	switch entries := obj[field].(type) {
	case map[string]any:
		for key := range entries {
			entries[key] = redactionMarkerValue
		}
	case map[string]string:
		for key := range entries {
			entries[key] = redactionMarkerValue
		}
	}
}

func redactSecretAnnotations(obj map[string]any) {
	metadata, ok := obj["metadata"].(map[string]any)
	if !ok {
		return
	}

	switch annotations := metadata["annotations"].(type) {
	case map[string]any:
		delete(annotations, lastAppliedAnnotation)
	case map[string]string:
		delete(annotations, lastAppliedAnnotation)
	}
}
