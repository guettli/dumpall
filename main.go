package main

import (
	"bytes"
	"context"
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
	clusterNamespace = "_cluster"
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
	"v1":                     {"events", "bindings", "componentstatuses"},
}

type options struct {
	outputDir         string
	quiet             bool
	dumpSecrets       bool
	dumpManagedFields bool
	removeOutdir      bool
	fileName          string
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
	pflag.StringVarP(&opts.fileName, "file-name", "f", "", "read --- sperated manifests from file")
	pflag.Parse()

	if opts.removeOutdir {
		if err := os.RemoveAll(opts.outputDir); err != nil {
			return fmt.Errorf("failed to remove out-dir %s: %w", opts.outputDir, err)
		}
	}

	if _, err := os.Stat(opts.outputDir); err == nil {
		return fmt.Errorf("output directory %q already exists. Use --remove-out-dir if you want to overwrite it", opts.outputDir)
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

func readYamlFromFile(fileName string, options *options) error {
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
		err = processUnstructured(u, isNamespaced, options)
		if err != nil {
			return fmt.Errorf("failed to process item %s: %w", u.GetName(), err)
		}
		fileCount++
	}
	fmt.Printf("Total files written: %d\n", fileCount)
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
		err := processUnstructured(item, isNamespaced, options)
		if err != nil {
			return fileCount, fmt.Errorf("failed to process item %s: %w", item.GetName(), err)
		}
		fileCount++
	}
	return fileCount, nil
}

func processUnstructured(item *unstructured.Unstructured, isNamespaced bool, options *options) error {
	ns := item.GetNamespace()
	if !isNamespaced {
		ns = clusterNamespace
	}
	gvk := item.GroupVersionKind()
	name := item.GetName()

	var dirPath string
	if gvk.Group == "" {
		dirPath = filepath.Join(options.outputDir, ns, gvk.Kind)
	} else {
		dirPath = filepath.Join(options.outputDir, ns, fmt.Sprintf("%s_%s", gvk.Group, gvk.Kind))
	}
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}
	filePath := filepath.Join(dirPath, fmt.Sprintf("%s.yaml", sanitizePath(name)))
	err := writeYAML(filePath, item.Object, options)
	if err != nil {
		fmt.Printf("Failed to write YAML for %s: %v\n", filePath, err)
	}
	return nil
}

func writeYAML(filePath string, obj map[string]interface{}, opts *options) error {
	metadata, ok := obj["metadata"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("metadata not found in object")
	}
	if !opts.dumpManagedFields {
		delete(metadata, "managedFields")
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer file.Close()

	encoder := yaml.NewEncoder(file)
	defer encoder.Close()
	encoder.SetIndent(2)
	if err := encoder.Encode(obj); err != nil {
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

var sanitizePathRegex = regexp.MustCompile(`[\\/:*?"'<>|!@#$%^&()+={}\[\];,]`)

func sanitizePath(path string) string {
	return sanitizePathRegex.ReplaceAllString(path, "_")
}
