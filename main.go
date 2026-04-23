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
	ignoreConfigUseCommon bool
	ignoreConfigFile      string
	fileName              string
	inputDir              string
	namespacesCSV         string
	nameRegex             string
	namespaceFilter       map[string]struct{}
	nameFilterEnabled     bool
	nameFilterRegex       *regexp.Regexp
	ignoreRules           []ignoreRule
}

type ignoreConfig struct {
	Rules []ignoreRuleFile `yaml:"rules"`
}

type ignoreRuleFile struct {
	Group     *string  `yaml:"group"`
	Kind      *string  `yaml:"kind"`
	Namespace *string  `yaml:"namespace"`
	Name      *string  `yaml:"name"`
	Fields    []string `yaml:"fields"`
}

type ignoreRule struct {
	GroupPattern     string
	KindPattern      string
	NamespacePattern string
	NamePattern      string
	Fields           []ignoreFieldPath
}

type ignoreFieldPath struct {
	segments []string
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
	pflag.BoolVar(&opts.ignoreConfigUseCommon, "ignore-config-use-common", false, "Use the embedded common ignore config")
	pflag.StringVar(&opts.ignoreConfigFile, "ignore-config", "", "Path to a YAML file with ignore rules")
	pflag.StringVarP(&opts.namespacesCSV, "namespaces", "n", "", "Comma-separated list of namespaces to dump")
	pflag.StringVarP(&opts.nameRegex, "name-regex", "x", "", "Only dump resources where metadata.name matches this regex")
	pflag.StringVarP(&opts.fileName, "file-name", "f", "", "Read YAML manifests from file (do not connect to api-server)")
	pflag.StringVarP(&opts.inputDir, "dir", "d", "", "Read YAML manifests recursively from directory (do not connect to api-server)")

	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s\nRead resources from the api-server, a YAML file, or a YAML directory tree and dump each resource to a file.\n\nSubcommands:\n  show-common-ignore-config   Print the embedded common ignore config\n", toolNameForUsageOutput)
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

	opts.fileName = strings.TrimSpace(opts.fileName)
	opts.inputDir = strings.TrimSpace(opts.inputDir)

	err = validateInputSources(opts.fileName, opts.inputDir)
	if err != nil {
		return err
	}

	ignoreRules, err := loadIgnoreRules(opts.ignoreConfigUseCommon, opts.ignoreConfigFile)
	if err != nil {
		return err
	}

	opts.ignoreRules = ignoreRules

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

	if opts.fileName != "" {
		return readYamlFromFile(opts.fileName, opts)
	}

	if opts.inputDir != "" {
		return readYamlFromDir(opts.inputDir, opts)
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

func validateInputSources(fileName string, inputDir string) error {
	if fileName != "" && inputDir != "" {
		return fmt.Errorf("--file-name and --dir are mutually exclusive")
	}

	return nil
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

func readFromAPIServer(client dynamic.Interface, resourceList []*meta.APIResourceList, opts *options) error {
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

			gvrJobs = append(gvrJobs, gvrListJob{
				groupVersion: apiGroup.GroupVersion,
				resourceName: resource.Name,
				gvr: schema.GroupVersionResource{
					Group:    getGroup(apiGroup.GroupVersion),
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

func enqueueGVRItems(client dynamic.Interface, job gvrListJob, opts *options, writeJobs chan<- resourceWriteJob) error {
	list, err := client.Resource(job.gvr).List(context.TODO(), meta.ListOptions{})
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
	pruneEmptyMaps(obj)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer file.Close()

	encoder := yaml.NewEncoder(file)
	defer encoder.Close()

	encoder.SetIndent(2)

	err = encoder.Encode(obj)
	if err != nil {
		return fmt.Errorf("failed to write YAML to file %s: %w", filePath, err)
	}

	return nil
}

func pruneEmptyMaps(obj map[string]any) {
	pruneEmptyMapsValue(obj)
}

func pruneEmptyMapsValue(value any) (any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			prunedChild, remove := pruneEmptyMapsValue(child)
			if remove {
				delete(typed, key)
				continue
			}

			typed[key] = prunedChild
		}

		return typed, len(typed) == 0
	case map[string]string:
		return typed, len(typed) == 0
	case []any:
		pruned := typed[:0]
		for _, child := range typed {
			prunedChild, remove := pruneEmptyMapsValue(child)
			if remove {
				continue
			}

			pruned = append(pruned, prunedChild)
		}

		return pruned, false
	case []map[string]any:
		pruned := typed[:0]
		for _, child := range typed {
			prunedChild, remove := pruneEmptyMapsValue(child)
			if remove {
				continue
			}

			pruned = append(pruned, prunedChild.(map[string]any))
		}

		return pruned, false
	case []map[string]string:
		pruned := typed[:0]
		for _, child := range typed {
			prunedChild, remove := pruneEmptyMapsValue(child)
			if remove {
				continue
			}

			pruned = append(pruned, prunedChild.(map[string]string))
		}

		return pruned, false
	default:
		return value, false
	}
}

func writeCommonIgnoreConfig(w io.Writer) error {
	_, err := w.Write(commonIgnoreConfig)
	if err != nil {
		return fmt.Errorf("failed to write embedded common ignore config: %w", err)
	}

	return nil
}

func loadIgnoreRules(useCommon bool, userConfigFile string) ([]ignoreRule, error) {
	trimmedFileName := strings.TrimSpace(userConfigFile)
	rules := make([]ignoreRule, 0)

	if useCommon {
		commonRules, err := parseIgnoreConfigBytes("embedded common ignore config", commonIgnoreConfig)
		if err != nil {
			return nil, err
		}

		rules = append(rules, commonRules...)
	}

	if trimmedFileName == "" {
		return rules, nil
	}

	userConfigBytes, err := os.ReadFile(trimmedFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read ignore config %s: %w", trimmedFileName, err)
	}

	userRules, err := parseIgnoreConfigBytes(trimmedFileName, userConfigBytes)
	if err != nil {
		return nil, err
	}

	return append(rules, userRules...), nil
}

func parseIgnoreConfigBytes(name string, data []byte) ([]ignoreRule, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)

	var cfg ignoreConfig
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse ignore config %s: %w", name, err)
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("failed to parse ignore config %s: expected a single YAML document", name)
		}

		return nil, fmt.Errorf("failed to parse ignore config %s: %w", name, err)
	}

	rules := make([]ignoreRule, 0, len(cfg.Rules))
	for idx := range cfg.Rules {
		rule, err := compileIgnoreRule(cfg.Rules[idx])
		if err != nil {
			return nil, fmt.Errorf("invalid ignore rule %d in %s: %w", idx+1, name, err)
		}

		rules = append(rules, rule)
	}

	return rules, nil
}

func compileIgnoreRule(fileRule ignoreRuleFile) (ignoreRule, error) {
	fields := make([]ignoreFieldPath, 0, len(fileRule.Fields))
	for _, field := range fileRule.Fields {
		trimmed := strings.TrimSpace(field)
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
			segments: segments,
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

			deleteFieldPath(obj, field.segments)
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

func deleteFieldPath(current any, segments []string) {
	if len(segments) == 0 {
		return
	}

	if segments[0] == "..." {
		deleteFieldPathRecursive(current, segments[1:])
		return
	}

	switch typed := current.(type) {
	case map[string]any:
		deleteFieldPathFromMap(typed, segments)
	case map[string]string:
		deleteFieldPathFromStringMap(typed, segments)
	case []any:
		for _, entry := range typed {
			deleteFieldPath(entry, segments)
		}
	case []map[string]any:
		for _, entry := range typed {
			deleteFieldPath(entry, segments)
		}
	case []map[string]string:
		for _, entry := range typed {
			deleteFieldPath(entry, segments)
		}
	}
}

func deleteFieldPathRecursive(current any, remaining []string) {
	if len(remaining) == 0 {
		return
	}

	deleteFieldPath(current, remaining)

	switch typed := current.(type) {
	case map[string]any:
		for _, value := range typed {
			deleteFieldPathRecursive(value, remaining)
		}
	case []any:
		for _, entry := range typed {
			deleteFieldPathRecursive(entry, remaining)
		}
	case []map[string]any:
		for _, entry := range typed {
			deleteFieldPathRecursive(entry, remaining)
		}
	case []map[string]string:
		for _, entry := range typed {
			deleteFieldPathRecursive(entry, remaining)
		}
	}
}

func deleteFieldPathFromMap(current map[string]any, segments []string) {
	keyPattern := segments[0]
	if !hasFieldSegmentWildcard(keyPattern) {
		if len(segments) == 1 {
			delete(current, keyPattern)
			return
		}

		next, ok := current[keyPattern]
		if !ok {
			return
		}

		deleteFieldPath(next, segments[1:])
		return
	}

	for key, next := range current {
		if !matchGlob(keyPattern, key) {
			continue
		}

		if len(segments) == 1 {
			delete(current, key)
			continue
		}

		deleteFieldPath(next, segments[1:])
	}
}

func deleteFieldPathFromStringMap(current map[string]string, segments []string) {
	if len(segments) != 1 {
		return
	}

	keyPattern := segments[0]
	if !hasFieldSegmentWildcard(keyPattern) {
		delete(current, keyPattern)
		return
	}

	for key := range current {
		if matchGlob(keyPattern, key) {
			delete(current, key)
		}
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

func shouldProcessItem(item *unstructured.Unstructured, isNamespaced bool, opts *options) bool {
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
