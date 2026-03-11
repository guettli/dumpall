package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
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
	outputDir         string
	quiet             bool
	dumpSecrets       bool
	dumpManagedFields bool
	removeOutdir      bool
	fileName          string
	namespacesCSV     string
	nameRegex         string
	namespaceFilter   map[string]struct{}
	nameFilterEnabled bool
	nameFilterRegex   *regexp.Regexp
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
	pflag.StringVarP(&opts.namespacesCSV, "namespaces", "n", "", "Comma-separated list of namespaces to dump")
	pflag.StringVarP(&opts.nameRegex, "name-regex", "x", "", "Only dump resources where metadata.name matches this regex")
	pflag.StringVarP(&opts.fileName, "file-name", "f", "", "read --- sperated manifests from file (do not connect to api-server)")

	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s\nRead all resources from the api-server and dumps each resource to a file.\n", toolNameForUsageOutput)
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

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)

	config, err := kubeconfig.ClientConfig()
	if err != nil {
		return fmt.Errorf("failed to get client config: %w", err)
	}

	// 80 concurrent requests were served in roughly 200ms
	// This means 400 requests in one second (to local kind cluster)
	// But why reduce this? I don't want people with better hardware
	// to wait for getting results from an api-server running at localhost
	config.QPS = 1000
	config.Burst = 1000

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

	var globalFileCount int64

	sort.Slice(resourceList, func(i int, j int) bool {
		return resourceList[i].GroupVersion < resourceList[j].GroupVersion
	})

	for _, apiGroup := range resourceList {
		sort.Slice(apiGroup.APIResources, func(i int, j int) bool {
			return apiGroup.APIResources[i].Name < apiGroup.APIResources[j].Name
		})

		for _, resource := range apiGroup.APIResources {
			skipSlice := toSkip[apiGroup.GroupVersion]
			if slices.Contains(skipSlice, resource.Name) {
				continue
			}

			gvr := schema.GroupVersionResource{
				Group:    getGroup(apiGroup.GroupVersion),
				Version:  getVersion(apiGroup.GroupVersion),
				Resource: resource.Name,
			}

			fileCount, err := processGVR(dynClient, gvr, resource.Namespaced, opts)
			if err != nil {
				fmt.Printf("Failed to process resource %q %s: %v\n", apiGroup.GroupVersion, resource.Name, err)
			}

			globalFileCount += fileCount
		}
	}

	if !opts.quiet {
		fmt.Printf("Total files written: %d\n", globalFileCount)
	}

	return nil
}

func readYamlFromFile(fileName string, opts *options) error {
	fileCount := int64(0)

	f, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	defer f.Close()

	bytes, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", fileName, err)
	}

	nodes, err := kio.FromBytes(bytes)
	if err != nil {
		return fmt.Errorf("failed to parse YAML: %w", err)
	}

	for i := range nodes {
		node := nodes[i]

		m, err := node.Map()
		if err != nil {
			return fmt.Errorf("failed to convert node to map: %w", err)
		}

		u := &unstructured.Unstructured{Object: m}

		ns, _, err := unstructured.NestedString(m, "metadata", "namespace")
		if err != nil {
			return fmt.Errorf("failed to get namespace for item %s: %w", u.GetName(), err)
		}

		isNamespaced := ns != ""

		if !shouldProcessItem(u, isNamespaced, opts) {
			continue
		}

		err = processUnstructured(u, isNamespaced, opts)
		if err != nil {
			return fmt.Errorf("failed to process item %s: %w", u.GetName(), err)
		}

		fileCount++
	}

	if !opts.quiet {
		fmt.Printf("Total files written: %d\n", fileCount)
	}

	return nil
}

func processGVR(client dynamic.Interface, gvr schema.GroupVersionResource, isNamespaced bool, options *options) (count int64, err error) {
	var fileCount int64

	list, err := client.Resource(gvr).List(context.TODO(), meta.ListOptions{})
	if err != nil {
		return fileCount, fmt.Errorf("failed to list resources for %s: %w", gvr.Resource, err)
	}

	for i := range list.Items {
		item := &list.Items[i]

		if !shouldProcessItem(item, isNamespaced, options) {
			continue
		}

		err := processUnstructured(item, isNamespaced, options)
		if err != nil {
			return fileCount, fmt.Errorf("failed to process item %s: %w", item.GetName(), err)
		}

		fileCount++
	}

	return fileCount, nil
}

func processUnstructured(item *unstructured.Unstructured, isNamespaced bool, opts *options) error {
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
		return fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}

	filePath := filepath.Join(dirPath, fmt.Sprintf("%s.yaml", sanitizePath(name)))

	err = writeYAML(filePath, item.Object, opts)
	if err != nil {
		fmt.Printf("Failed to write YAML for %s: %v\n", filePath, err)
	}

	return nil
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

	if !opts.quiet {
		fmt.Printf("Written: %s\n", filePath)
	}

	return nil
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
