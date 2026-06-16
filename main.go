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
	"time"

	"github.com/gavv/cobradoc"
	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/kustomize/kyaml/kio"
	yaml "sigs.k8s.io/yaml/goyaml.v3"
)

const (
	clusterNamespace       = "_cluster"
	redactionMarkerValue   = "REDCATED-BY-DUMPALL"
	toolNameForUsageOutput = "dumpall"
	lastAppliedAnnotation  = "kubectl.kubernetes.io/last-applied-configuration"
	dumpMetaFileName       = "_dumpall_meta.yaml"

	kindNamespace      = "Namespace"
	kindClusterRole    = "ClusterRole"
	kindServiceAccount = "ServiceAccount"
	kindConfigMap      = "ConfigMap"
	kindSecret         = "Secret"

	appsV1            = "apps/v1"
	rbacGroup         = "rbac.authorization.k8s.io"
	bootstrappingKey  = "kubernetes.io/bootstrapping"
	rbacDefaultsValue = "rbac-defaults"
	kubeRootCAName    = "kube-root-ca.crt"

	fieldMetadata = "metadata"
	deepWildcard  = "..."
	fieldRules    = "rules"
	defaultName   = "default"
)

//go:embed common-ignore-config.yaml
var commonIgnoreConfig []byte

var toSkip = map[string][]string{
	appsV1: {"replicasets"},
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

type dumpMeta struct {
	DumpedAt    time.Time `yaml:"dumpedAt"`
	ClusterHost string    `yaml:"clusterHost"`
}

type options struct {
	outputDir             string
	quiet                 bool
	dumpSecrets           bool
	dumpManagedFields     bool
	removeOutdir          bool
	overwriteOutdir       bool
	skipOwned             bool
	ignoreConfigUseCommon bool
	ignoreConfigFile      string
	readYamlFrom          string
	readResourceNamesFrom string
	namespacesCSV         string
	kindFilterCSV         string
	nameRegex             string
	skipNameGlob          string
	excludeNamespacesCSV  string
	comment               string
	fileName              string
	dir                   string
	namespaceFilter       map[string]struct{}
	kindFilter            []string
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
	Group     *string                `yaml:"group"`
	Kind      *string                `yaml:"kind"`
	Namespace *string                `yaml:"namespace"`
	Name      *string                `yaml:"name"`
	Fields    []ignoreFieldEntryFile `yaml:"fields"`
}

// ignoreFieldEntryFile is a single entry in the fields list of a removeFields rule.
// It is either a plain field path string, or a map with "path" and optional "value",
// "omitempty", or "omitIfEqualToSibling" keys. All three optional keys are mutually exclusive.
type ignoreFieldEntryFile struct {
	Path             string
	Value            any
	valueConstrained bool
	// OmitEmpty removes the field only when its value is nil, an empty map, an empty
	// slice, or an empty string — mirroring the json:",omitempty" / kubebuilder
	// +optional convention. Mutually exclusive with a value constraint.
	OmitEmpty bool
	// OmitIfEqualToSibling removes the field only when its value equals the value of
	// the named sibling field in the same map object. Useful for defaulted fields like
	// spec.ports.targetPort which Kubernetes sets to the same value as spec.ports.port.
	// Mutually exclusive with value and omitempty.
	OmitIfEqualToSibling string
}

func (e *ignoreFieldEntryFile) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		e.Path = value.Value
		return nil
	case yaml.MappingNode:
		hasValue, hasOmitEmpty, hasOmitIfEqualToSibling := false, false, false

		for i := 0; i < len(value.Content)-1; i += 2 {
			key := value.Content[i].Value
			switch key {
			case "path":
			case "value":
				hasValue = true
			case "omitempty":
				hasOmitEmpty = true
			case "omitIfEqualToSibling":
				hasOmitIfEqualToSibling = true
			default:
				return fmt.Errorf("unknown field %q in field entry (allowed: path, value, omitempty, omitIfEqualToSibling)", key)
			}
		}

		modifiers := 0

		for _, has := range []bool{hasValue, hasOmitEmpty, hasOmitIfEqualToSibling} {
			if has {
				modifiers++
			}
		}

		if modifiers > 1 {
			return fmt.Errorf("field entry can only set one of 'value', 'omitempty', or 'omitIfEqualToSibling'")
		}

		var m struct {
			Path                 string `yaml:"path"`
			Value                any    `yaml:"value"`
			OmitEmpty            bool   `yaml:"omitempty"`
			OmitIfEqualToSibling string `yaml:"omitIfEqualToSibling"`
		}

		err := value.Decode(&m)
		if err != nil {
			return fmt.Errorf("failed to decode field entry: %w", err)
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

		if hasOmitIfEqualToSibling {
			if strings.TrimSpace(m.OmitIfEqualToSibling) == "" {
				return fmt.Errorf("field entry 'omitIfEqualToSibling' must not be empty")
			}

			e.OmitIfEqualToSibling = m.OmitIfEqualToSibling
		}

		return nil
	case yaml.DocumentNode, yaml.SequenceNode, yaml.AliasNode:
		return fmt.Errorf("field entry must be a string or a map with 'path', 'value', or 'omitempty' keys")
	}

	return fmt.Errorf("field entry must be a string or a map with 'path', 'value', or 'omitempty' keys")
}

type ignoreRule struct {
	GroupPattern     string
	KindPattern      string
	NamespacePattern string
	NamePattern      string
	Fields           []ignoreFieldPath
}

type ignoreFieldPath struct {
	segments             []string
	value                any
	valueConstrained     bool
	omitEmpty            bool
	omitIfEqualToSibling string
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

type fileValue string

func (v *fileValue) String() string     { return string(*v) }
func (v *fileValue) Set(s string) error { *v = fileValue(s); return nil }
func (v *fileValue) Type() string       { return "file" }

type dirValue string

func (v *dirValue) String() string     { return string(*v) }
func (v *dirValue) Set(s string) error { *v = dirValue(s); return nil }
func (v *dirValue) Type() string       { return "dir" }

type pathValue string

func (v *pathValue) String() string     { return string(*v) }
func (v *pathValue) Set(s string) error { *v = pathValue(s); return nil }
func (v *pathValue) Type() string       { return "path" }

func main() {
	err := buildRootCmd().Execute()
	if err != nil {
		os.Exit(1)
	}
}

func buildRootCmd() *cobra.Command {
	opts := &options{outputDir: "out"}

	rootCmd := &cobra.Command{
		Use:          toolNameForUsageOutput,
		Short:        "Dump all Kubernetes resources to YAML files",
		Long:         "Read resources from the api-server (or a YAML file/directory via --read-yaml-from) and dump each resource to a file.",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
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

			err := finalizeOpts(opts)
			if err != nil {
				return err
			}

			if opts.removeOutdir {
				err := os.RemoveAll(opts.outputDir)
				if err != nil {
					return fmt.Errorf("failed to remove out-dir %s: %w", opts.outputDir, err)
				}
			}

			return runDump(opts)
		},
	}

	rootCmd.Flags().VarP((*dirValue)(&opts.outputDir), "out-dir", "o", "Output directory (must not exist)")
	rootCmd.Flags().BoolVarP(&opts.quiet, "quiet", "q", false, "Quiet, suppress output")
	rootCmd.Flags().BoolVarP(&opts.dumpSecrets, "dump-secrets", "s", false, "Dump secrets (disabled by default)")
	rootCmd.Flags().BoolVarP(&opts.dumpManagedFields, "dump-managed-fields", "m", false, "Dump managed fields (disabled by default)")
	rootCmd.Flags().BoolVarP(&opts.removeOutdir, "remove-out-dir", "r", false, "Remove out-dir before dumping (disabled by default)")
	rootCmd.Flags().BoolVarP(&opts.skipOwned, "skip-owned", "O", false, "Skip resources that have a controlling owner reference (e.g., Pods owned by a ReplicaSet) or that Kubernetes autogenerates from other resources (e.g., aggregated ClusterRoles)")
	rootCmd.Flags().BoolVar(&opts.ignoreConfigUseCommon, "ignore-config-use-common", false, "Use the embedded common ignore config")
	rootCmd.Flags().Var((*fileValue)(&opts.ignoreConfigFile), "ignore-config", "Path to a YAML file with ignore rules")
	rootCmd.Flags().StringVarP(&opts.namespacesCSV, "namespaces", "n", "", "Comma-separated list of namespaces to dump")
	rootCmd.Flags().StringVar(&opts.kindFilterCSV, "kind", "", "Comma-separated list of kind globs to dump (e.g. 'ConfigMap,Secret,Cluster*')")
	rootCmd.Flags().StringVarP(&opts.nameRegex, "name-regex", "x", "", "Only dump resources where metadata.name matches this regex")
	rootCmd.Flags().StringVar(&opts.skipNameGlob, "skip-name-glob", "", "Skip resources where metadata.name matches this glob (e.g. 'foo-*' skips names starting with 'foo-')")
	rootCmd.Flags().StringVar(&opts.excludeNamespacesCSV, "exclude-namespaces", "", "Comma-separated list of namespace globs to fully exclude (e.g. 'foo-*,test-*'). Drops the Namespace object plus all resources inside; uses fieldSelector to skip those namespaces api-server-side")
	rootCmd.Flags().Var((*pathValue)(&opts.readYamlFrom), "read-yaml-from", "Read YAML manifests from a file or directory instead of connecting to the api-server. Useful for normalizing existing YAML files for better diffing.")
	rootCmd.Flags().Var((*pathValue)(&opts.readResourceNamesFrom), "read-resource-names-from", "Read resource identifiers (kind/namespace/name) from a YAML file or directory and dump only those resources. Useful to dump a specific subset of cluster resources from the api-server.")
	rootCmd.Flags().StringVar(&opts.comment, "comment", "", "Additional comment line to add at the top of each output YAML file")
	rootCmd.Flags().StringVarP(&opts.fileName, "file-name", "f", "", "Alias for --read-yaml-from (hidden)")
	rootCmd.Flags().StringVar(&opts.dir, "dir", "", "Alias for --read-yaml-from (hidden)")
	_ = rootCmd.Flags().MarkHidden("file-name")
	_ = rootCmd.Flags().MarkHidden("dir")

	rootCmd.AddCommand(buildDiffCmd())
	rootCmd.AddCommand(buildCheckNormalizedCmd())
	rootCmd.AddCommand(buildTopChurnCmd())
	rootCmd.AddCommand(buildShowCommonIgnoreConfigCmd())
	rootCmd.AddCommand(buildVersionCmd())
	rootCmd.AddCommand(buildGendocsCmd())

	return rootCmd
}

func buildShowCommonIgnoreConfigCmd() *cobra.Command {
	return &cobra.Command{
		Use:          "show-common-ignore-config",
		Short:        "Print the embedded common ignore config",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return writeCommonIgnoreConfig(os.Stdout)
		},
	}
}

func buildVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:          "version",
		Short:        "Print the version",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			fmt.Println(getBuildVersion())
			return nil
		},
	}
}

func buildGendocsCmd() *cobra.Command {
	return &cobra.Command{
		Use:          "gendocs",
		Short:        "Generate usage.md from the command tree",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			b := &bytes.Buffer{}

			err := cobradoc.WriteDocument(b, cmd.Root(), cobradoc.Markdown, cobradoc.Options{})
			if err != nil {
				return fmt.Errorf("generating docs: %w", err)
			}

			usageFile := "usage.md"

			err = os.WriteFile(usageFile, b.Bytes(), 0o600)
			if err != nil {
				return fmt.Errorf("writing %s: %w", usageFile, err)
			}

			fmt.Printf("Created %q\n", usageFile)

			return nil
		},
	}
}

func finalizeOpts(opts *options) error {
	namespaceFilter, err := parseNamespaceFilter(opts.namespacesCSV)
	if err != nil {
		return err
	}

	opts.namespaceFilter = namespaceFilter

	kindFilter, err := parseKindFilter(opts.kindFilterCSV)
	if err != nil {
		return err
	}

	opts.kindFilter = kindFilter

	nameFilterRegex, nameFilterEnabled, err := parseNameFilterRegex(opts.nameRegex)
	if err != nil {
		return err
	}

	opts.nameFilterEnabled = nameFilterEnabled
	opts.nameFilterRegex = nameFilterRegex

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

	return nil
}

func runDump(opts *options) error {
	_, err := os.Stat(opts.outputDir)
	if err == nil && !opts.overwriteOutdir {
		return fmt.Errorf("output directory %q already exists. Use --remove-out-dir if you want to overwrite it", opts.outputDir)
	}

	if err != nil && !errors.Is(err, os.ErrNotExist) {
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

	err = readFromAPIServer(dynClient, resourceList, opts)
	if err != nil {
		return err
	}

	return writeDumpMeta(opts.outputDir, config.Host)
}

var errDiffsFound = errors.New("differences found")

type diffOpts struct {
	readYamlFrom         string
	namespacesCSV        string
	kindFilterCSV        string
	nameRegex            string
	skipNameGlob         string
	excludeNamespacesCSV string
	ignoreConfigFile     string
	dumpSecrets          bool
	skipOwned            bool
	quiet                bool
	noCommonIgnoreConfig bool
}

func buildDiffCmd() *cobra.Command {
	var d diffOpts

	cmd := &cobra.Command{
		Use:          "diff <local-dump-dir>",
		Short:        "Diff the current cluster state against a local dump",
		Long:         "Diff the current cluster state against a local dump directory.\nBoth sides are normalized before comparing (common ignore config applied by default).",
		SilenceUsage: true,
		Args:         cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runDiffCore(d, args[0])
		},
	}

	cmd.Flags().StringVar(&d.readYamlFrom, "read-yaml-from", "", "Read YAML from file/dir (source A) instead of connecting to cluster")
	cmd.Flags().StringVarP(&d.namespacesCSV, "namespaces", "n", "", "Comma-separated list of namespaces to compare")
	cmd.Flags().StringVar(&d.kindFilterCSV, "kind", "", "Comma-separated list of kind globs to compare (e.g. 'ConfigMap,Secret')")
	cmd.Flags().StringVarP(&d.nameRegex, "name-regex", "x", "", "Only compare resources where metadata.name matches this regex")
	cmd.Flags().StringVar(&d.skipNameGlob, "skip-name-glob", "", "Skip resources where metadata.name matches this glob")
	cmd.Flags().StringVar(&d.excludeNamespacesCSV, "exclude-namespaces", "", "Comma-separated list of namespace globs to exclude")
	cmd.Flags().Var((*fileValue)(&d.ignoreConfigFile), "ignore-config", "Path to a YAML file with ignore rules")
	cmd.Flags().BoolVar(&d.noCommonIgnoreConfig, "no-common-ignore-config", false, "Disable the embedded common ignore config (compare raw resources)")
	cmd.Flags().BoolVarP(&d.dumpSecrets, "dump-secrets", "s", false, "Include secret values in comparison")
	cmd.Flags().BoolVarP(&d.skipOwned, "skip-owned", "O", false, "Skip resources with a controlling owner reference")
	cmd.Flags().BoolVarP(&d.quiet, "quiet", "q", true, "Suppress progress output")

	return cmd
}

// runDiff parses raw flag args and delegates to runDiffCore.
// Kept for direct test calls (e.g. runDiff([]string{"--read-yaml-from", ...})).
func runDiff(args []string) error {
	fs := pflag.NewFlagSet("diff", pflag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var d diffOpts

	fs.StringVar(&d.readYamlFrom, "read-yaml-from", "", "Read YAML from file/dir (source A) instead of connecting to cluster")
	fs.StringVarP(&d.namespacesCSV, "namespaces", "n", "", "Comma-separated list of namespaces to compare")
	fs.StringVar(&d.kindFilterCSV, "kind", "", "Comma-separated list of kind globs to compare (e.g. 'ConfigMap,Secret')")
	fs.StringVarP(&d.nameRegex, "name-regex", "x", "", "Only compare resources where metadata.name matches this regex")
	fs.StringVar(&d.skipNameGlob, "skip-name-glob", "", "Skip resources where metadata.name matches this glob")
	fs.StringVar(&d.excludeNamespacesCSV, "exclude-namespaces", "", "Comma-separated list of namespace globs to exclude")
	fs.Var((*fileValue)(&d.ignoreConfigFile), "ignore-config", "Path to a YAML file with ignore rules")
	fs.BoolVar(&d.noCommonIgnoreConfig, "no-common-ignore-config", false, "Disable the embedded common ignore config (compare raw resources)")
	fs.BoolVarP(&d.dumpSecrets, "dump-secrets", "s", false, "Include secret values in comparison")
	fs.BoolVarP(&d.skipOwned, "skip-owned", "O", false, "Skip resources with a controlling owner reference")
	fs.BoolVarP(&d.quiet, "quiet", "q", true, "Suppress progress output")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s diff [flags] <local-dump-dir>\n\nDiff the current cluster state against a local dump directory.\nBoth sides are normalized before comparing (common ignore config applied by default).\n\nFlags:\n", toolNameForUsageOutput)
		fs.PrintDefaults()
	}

	err := fs.Parse(args)
	if err != nil {
		return fmt.Errorf("parsing flags: %w", err)
	}

	if len(fs.Args()) != 1 {
		fs.Usage()
		return fmt.Errorf("diff requires exactly one positional argument: the local dump directory")
	}

	return runDiffCore(d, fs.Args()[0])
}

func runDiffCore(d diffOpts, localDumpDir string) error {
	tempA, err := os.MkdirTemp("", "dumpall-diff-a-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	defer os.RemoveAll(tempA)

	tempB, err := os.MkdirTemp("", "dumpall-diff-b-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	defer os.RemoveAll(tempB)

	labelA := "cluster"
	if d.readYamlFrom != "" {
		labelA = d.readYamlFrom
	}

	buildOpts := func(outputDir, readFrom string) *options {
		return &options{
			outputDir:             outputDir,
			readYamlFrom:          strings.TrimSpace(readFrom),
			namespacesCSV:         d.namespacesCSV,
			kindFilterCSV:         d.kindFilterCSV,
			nameRegex:             d.nameRegex,
			skipNameGlob:          d.skipNameGlob,
			excludeNamespacesCSV:  d.excludeNamespacesCSV,
			ignoreConfigFile:      d.ignoreConfigFile,
			dumpSecrets:           d.dumpSecrets,
			skipOwned:             d.skipOwned,
			ignoreConfigUseCommon: !d.noCommonIgnoreConfig,
			quiet:                 d.quiet,
			overwriteOutdir:       true,
		}
	}

	optsA := buildOpts(tempA, d.readYamlFrom)

	err = finalizeOpts(optsA)
	if err != nil {
		return fmt.Errorf("configuring source A: %w", err)
	}

	err = runDump(optsA)
	if err != nil {
		return fmt.Errorf("normalizing %s: %w", labelA, err)
	}

	optsB := buildOpts(tempB, localDumpDir)

	err = finalizeOpts(optsB)
	if err != nil {
		return fmt.Errorf("configuring source B: %w", err)
	}

	err = runDump(optsB)
	if err != nil {
		return fmt.Errorf("normalizing %s: %w", localDumpDir, err)
	}

	return compareDirs(tempA, labelA, tempB, localDumpDir, strings.TrimSpace(d.readYamlFrom))
}

type checkNormalizedOpts struct {
	namespacesCSV        string
	kindFilterCSV        string
	nameRegex            string
	skipNameGlob         string
	excludeNamespacesCSV string
	ignoreConfigFile     string
	dumpSecrets          bool
	skipOwned            bool
	quiet                bool
	noCommonIgnoreConfig bool
}

func buildCheckNormalizedCmd() *cobra.Command {
	var c checkNormalizedOpts

	cmd := &cobra.Command{
		Use:          "check-normalized <yaml-file-or-dir>",
		Short:        "Check whether a YAML file or directory is already normalized",
		Long:         "Check whether a YAML file or directory is already normalized.\nExits 0 if already normalized, 1 if normalization would change it.",
		SilenceUsage: true,
		Args:         cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runCheckNormalizedCore(c, args[0])
		},
	}

	cmd.Flags().StringVarP(&c.namespacesCSV, "namespaces", "n", "", "Comma-separated list of namespaces to filter")
	cmd.Flags().StringVar(&c.kindFilterCSV, "kind", "", "Comma-separated list of kind globs to filter (e.g. 'ConfigMap,Secret')")
	cmd.Flags().StringVarP(&c.nameRegex, "name-regex", "x", "", "Only check resources where metadata.name matches this regex")
	cmd.Flags().StringVar(&c.skipNameGlob, "skip-name-glob", "", "Skip resources where metadata.name matches this glob")
	cmd.Flags().StringVar(&c.excludeNamespacesCSV, "exclude-namespaces", "", "Comma-separated list of namespace globs to exclude")
	cmd.Flags().Var((*fileValue)(&c.ignoreConfigFile), "ignore-config", "Path to a YAML file with additional ignore rules")
	cmd.Flags().BoolVar(&c.noCommonIgnoreConfig, "no-common-ignore-config", false, "Disable the embedded common ignore config")
	cmd.Flags().BoolVarP(&c.dumpSecrets, "dump-secrets", "s", false, "Include secret values")
	cmd.Flags().BoolVarP(&c.skipOwned, "skip-owned", "O", false, "Skip resources with a controlling owner reference")
	cmd.Flags().BoolVarP(&c.quiet, "quiet", "q", true, "Suppress progress output")

	return cmd
}

func runCheckNormalizedCore(c checkNormalizedOpts, inputPath string) error {
	// tempRaw: restructured without any ignore rules (the "before" side)
	tempRaw, err := os.MkdirTemp("", "dumpall-raw-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	defer os.RemoveAll(tempRaw)

	// tempNorm: restructured with ignore rules applied (the "after" side)
	tempNorm, err := os.MkdirTemp("", "dumpall-norm-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	defer os.RemoveAll(tempNorm)

	buildOpts := func(outputDir string, useCommon bool, customConfig string) *options {
		return &options{
			outputDir:             outputDir,
			readYamlFrom:          strings.TrimSpace(inputPath),
			namespacesCSV:         c.namespacesCSV,
			kindFilterCSV:         c.kindFilterCSV,
			nameRegex:             c.nameRegex,
			skipNameGlob:          c.skipNameGlob,
			excludeNamespacesCSV:  c.excludeNamespacesCSV,
			ignoreConfigFile:      customConfig,
			dumpSecrets:           c.dumpSecrets,
			skipOwned:             c.skipOwned,
			ignoreConfigUseCommon: useCommon,
			quiet:                 c.quiet,
			overwriteOutdir:       true,
		}
	}

	optsRaw := buildOpts(tempRaw, false, "")

	err = finalizeOpts(optsRaw)
	if err != nil {
		return fmt.Errorf("configuring raw pass: %w", err)
	}

	err = runDump(optsRaw)
	if err != nil {
		return fmt.Errorf("reading %s: %w", inputPath, err)
	}

	optsNorm := buildOpts(tempNorm, !c.noCommonIgnoreConfig, c.ignoreConfigFile)

	err = finalizeOpts(optsNorm)
	if err != nil {
		return fmt.Errorf("configuring normalized pass: %w", err)
	}

	err = runDump(optsNorm)
	if err != nil {
		return fmt.Errorf("normalizing %s: %w", inputPath, err)
	}

	// raw = old (---), normalized = new (+++) so removed fields show as - lines
	return compareDirs(tempNorm, "normalized", tempRaw, inputPath, "")
}

// compareDirs diffs two normalized dump directories using gotextdiff.
// sourceA is the original source path for side A (empty when dumped from cluster).
// When sourceA is set, a copy-pasteable "diff fileA fileB" line is printed; otherwise
// only the relative path is printed (cluster files don't persist on disk).
func compareDirs(dirA, labelA, dirB, labelB, sourceA string) error {
	filesA, err := findYAMLFiles(dirA)
	if err != nil {
		return err
	}

	filesB, err := findYAMLFiles(dirB)
	if err != nil {
		return err
	}

	relToAbsA := make(map[string]string, len(filesA))

	for _, f := range filesA {
		rel, err := filepath.Rel(dirA, f)
		if err != nil {
			return fmt.Errorf("calculating relative path: %w", err)
		}

		relToAbsA[rel] = f
	}

	relToAbsB := make(map[string]string, len(filesB))

	for _, f := range filesB {
		rel, err := filepath.Rel(dirB, f)
		if err != nil {
			return fmt.Errorf("calculating relative path: %w", err)
		}

		relToAbsB[rel] = f
	}

	allRels := make(map[string]struct{}, len(relToAbsA)+len(relToAbsB))

	for rel := range relToAbsA {
		allRels[rel] = struct{}{}
	}

	for rel := range relToAbsB {
		allRels[rel] = struct{}{}
	}

	sortedRels := make([]string, 0, len(allRels))

	for rel := range allRels {
		sortedRels = append(sortedRels, rel)
	}

	sort.Strings(sortedRels)

	hasDiffs := false

	for _, rel := range sortedRels {
		absA, inA := relToAbsA[rel]
		absB, inB := relToAbsB[rel]

		switch {
		case inA && !inB:
			// File exists in cluster (new) but not in local (old): new file.
			fmt.Printf("new in %s: %s\n", labelA, rel)

			hasDiffs = true
		case !inA && inB:
			// File exists in local (old) but not in cluster (new): deleted file.
			fmt.Printf("deleted from %s: %s\n", labelA, rel)

			hasDiffs = true
		default:
			contentA, err := os.ReadFile(absA)
			if err != nil {
				return fmt.Errorf("failed to read %s: %w", absA, err)
			}

			contentB, err := os.ReadFile(absB)
			if err != nil {
				return fmt.Errorf("failed to read %s: %w", absB, err)
			}

			// local (B) is old/from (---), cluster (A) is new/to (+++).
			strOld := string(contentB)
			strNew := string(contentA)

			if strOld != strNew {
				hasDiffs = true

				if sourceA != "" {
					fmt.Printf("\ndiff %s %s\n", filepath.Join(labelB, rel), filepath.Join(sourceA, rel))
				} else {
					fmt.Printf("\n%s\n", rel)
				}

				edits := myers.ComputeEdits(span.URIFromPath(absB), strOld, strNew)
				diff := fmt.Sprint(gotextdiff.ToUnified(filepath.Join(labelB, rel), filepath.Join(labelA, rel), strOld, edits))
				fmt.Print(diff)
			}
		}
	}

	if hasDiffs {
		return errDiffsFound
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

		if filepath.Base(currentPath) == dumpMetaFileName {
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

		ns, _, err := unstructured.NestedString(m, fieldMetadata, "namespace")
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

func validateKindFilterMatchesResources(kindFilter []string, resourceList []*meta.APIResourceList) error {
	if len(kindFilter) == 0 {
		return nil
	}

	for _, apiGroup := range resourceList {
		for _, resource := range apiGroup.APIResources {
			if matchesAnyGlob(kindFilter, resource.Kind) {
				return nil
			}
		}
	}

	return fmt.Errorf("--kind: no resource types found matching: %s (use `kubectl api-resources` to list available kinds)", strings.Join(kindFilter, ", "))
}

func readFromAPIServer(client dynamic.Interface, resourceList []*meta.APIResourceList, opts *options) error {
	err := resolveExcludeNamespaceFieldSelector(client, opts)
	if err != nil {
		return err
	}

	err = validateNamespaceFilter(client, opts)
	if err != nil {
		return err
	}

	err = validateKindFilterMatchesResources(opts.kindFilter, resourceList)
	if err != nil {
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

			if len(opts.kindFilter) > 0 && !matchesAnyGlob(opts.kindFilter, resource.Kind) {
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
		parts = append(parts, fieldMetadata+".namespace!="+name)
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
	workerCount := max(runtime.GOMAXPROCS(0), 2)

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
	metadata, ok := obj[fieldMetadata].(map[string]any)
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
	metadata, ok := obj[fieldMetadata].(map[string]any)
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
	err := rejectLegacyIgnoreConfigKeys(name, data)
	if err != nil {
		return nil, nil, nil, err
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)

	var cfg ignoreConfig

	err = decoder.Decode(&cfg)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse ignore config %s: %w", name, err)
	}

	var extra any

	err = decoder.Decode(&extra)
	if !errors.Is(err, io.EOF) {
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

	for entry := range strings.SplitSeq(csv, ",") {
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

		err := validateGlobPattern(trimmed)
		if err != nil {
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
			if segment == deepWildcard || !hasFieldSegmentWildcard(segment) {
				continue
			}

			err := validateGlobPattern(segment)
			if err != nil {
				return ignoreRule{}, fmt.Errorf("invalid field path %q: invalid segment glob %q: %w", trimmed, segment, err)
			}
		}

		fields = append(fields, ignoreFieldPath{
			segments:             segments,
			value:                field.Value,
			valueConstrained:     field.valueConstrained,
			omitEmpty:            field.OmitEmpty,
			omitIfEqualToSibling: field.OmitIfEqualToSibling,
		})
	}

	if len(fields) == 0 {
		return ignoreRule{}, fmt.Errorf("fields must not be empty")
	}

	groupPattern := patternOrWildcard(fileRule.Group)

	err := validateGlobPattern(groupPattern)
	if err != nil {
		return ignoreRule{}, fmt.Errorf("invalid group glob %q: %w", groupPattern, err)
	}

	kindPattern := patternOrWildcard(fileRule.Kind)

	err = validateGlobPattern(kindPattern)
	if err != nil {
		return ignoreRule{}, fmt.Errorf("invalid kind glob %q: %w", kindPattern, err)
	}

	namespacePattern := patternOrWildcard(fileRule.Namespace)

	err = validateGlobPattern(namespacePattern)
	if err != nil {
		return ignoreRule{}, fmt.Errorf("invalid namespace glob %q: %w", namespacePattern, err)
	}

	namePattern := patternOrWildcard(fileRule.Name)

	err = validateGlobPattern(namePattern)
	if err != nil {
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

	if yaml.Unmarshal(data, &preview) == nil {
		legacy := []struct {
			oldKey string
			newKey string
		}{
			{fieldRules, "removeFields"},
			{"skip", "skipResources"},
		}

		for _, entry := range legacy {
			if _, ok := preview[entry.oldKey]; ok {
				return fmt.Errorf("ignore config %s uses legacy key %q — rename it to %q", name, entry.oldKey, entry.newKey)
			}
		}
	}

	return nil
}

func compileSkipRule(fileRule skipRuleFile) (skipRule, error) {
	if !skipRuleHasMatcher(fileRule) {
		return skipRule{}, fmt.Errorf("skip rule must set at least one of group, kind, namespace, name")
	}

	groupPattern := patternOrWildcard(fileRule.Group)

	err := validateGlobPattern(groupPattern)
	if err != nil {
		return skipRule{}, fmt.Errorf("invalid group glob %q: %w", groupPattern, err)
	}

	kindPattern := patternOrWildcard(fileRule.Kind)

	err = validateGlobPattern(kindPattern)
	if err != nil {
		return skipRule{}, fmt.Errorf("invalid kind glob %q: %w", kindPattern, err)
	}

	namespacePattern := patternOrWildcard(fileRule.Namespace)

	err = validateGlobPattern(namespacePattern)
	if err != nil {
		return skipRule{}, fmt.Errorf("invalid namespace glob %q: %w", namespacePattern, err)
	}

	namePattern := patternOrWildcard(fileRule.Name)

	err = validateGlobPattern(namePattern)
	if err != nil {
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
	if err != nil {
		return fmt.Errorf("invalid glob pattern: %w", err)
	}

	return nil
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
		case fieldPath[i:] == deepWildcard || strings.HasPrefix(fieldPath[i:], deepWildcard):
			if current.Len() > 0 {
				segments = append(segments, current.String())
				current.Reset()
			}

			segments = append(segments, deepWildcard)
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
		if len(segments) > 0 && segments[len(segments)-1] == deepWildcard {
			return nil, fmt.Errorf("path must not end with the '...' wildcard segment")
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

			deleteFieldPath(obj, field.segments, field.value, field.valueConstrained, field.omitEmpty, field.omitIfEqualToSibling)
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

func matchesAnyGlob(patterns []string, value string) bool {
	for _, pattern := range patterns {
		if matchGlob(pattern, value) {
			return true
		}
	}

	return false
}

func matchGlob(pattern string, value string) bool {
	matched, err := path.Match(pattern, value)
	if err != nil {
		return false
	}

	return matched
}

func isManagedFieldsPath(segments []string) bool {
	return len(segments) == 2 && segments[0] == fieldMetadata && segments[1] == "managedFields"
}

func deleteFieldPath(current any, segments []string, constraintValue any, constrained bool, omitEmpty bool, omitIfEqualToSibling string) {
	if len(segments) == 0 {
		return
	}

	if segments[0] == deepWildcard {
		deleteFieldPathRecursive(current, segments[1:], constraintValue, constrained, omitEmpty, omitIfEqualToSibling)
		return
	}

	switch typed := current.(type) {
	case map[string]any:
		deleteFieldPathFromMap(typed, segments, constraintValue, constrained, omitEmpty, omitIfEqualToSibling)
	case map[string]string:
		deleteFieldPathFromStringMap(typed, segments, constraintValue, constrained, omitEmpty)
	case []any:
		for _, entry := range typed {
			deleteFieldPath(entry, segments, constraintValue, constrained, omitEmpty, omitIfEqualToSibling)
		}
	case []map[string]any:
		for _, entry := range typed {
			deleteFieldPath(entry, segments, constraintValue, constrained, omitEmpty, omitIfEqualToSibling)
		}
	case []map[string]string:
		for _, entry := range typed {
			deleteFieldPath(entry, segments, constraintValue, constrained, omitEmpty, omitIfEqualToSibling)
		}
	}
}

func deleteFieldPathRecursive(current any, remaining []string, constraintValue any, constrained bool, omitEmpty bool, omitIfEqualToSibling string) {
	if len(remaining) == 0 {
		return
	}

	deleteFieldPath(current, remaining, constraintValue, constrained, omitEmpty, omitIfEqualToSibling)

	switch typed := current.(type) {
	case map[string]any:
		for _, value := range typed {
			deleteFieldPathRecursive(value, remaining, constraintValue, constrained, omitEmpty, omitIfEqualToSibling)
		}
	case []any:
		for _, entry := range typed {
			deleteFieldPathRecursive(entry, remaining, constraintValue, constrained, omitEmpty, omitIfEqualToSibling)
		}
	case []map[string]any:
		for _, entry := range typed {
			deleteFieldPathRecursive(entry, remaining, constraintValue, constrained, omitEmpty, omitIfEqualToSibling)
		}
	case []map[string]string:
		for _, entry := range typed {
			deleteFieldPathRecursive(entry, remaining, constraintValue, constrained, omitEmpty, omitIfEqualToSibling)
		}
	}
}

func shouldDeleteEntry(value, constraintValue any, constrained, omitEmpty bool) bool {
	if constrained && !valueMatches(value, constraintValue) {
		return false
	}

	if omitEmpty && !isEmpty(value) {
		return false
	}

	return true
}

func shouldDeleteLeaf(current map[string]any, key string, constraintValue any, constrained bool, omitEmpty bool, omitIfEqualToSibling string) bool {
	if omitIfEqualToSibling != "" {
		sibling, ok := current[omitIfEqualToSibling]
		return ok && valueMatches(current[key], sibling)
	}

	return shouldDeleteEntry(current[key], constraintValue, constrained, omitEmpty)
}

func deleteFieldPathFromMap(current map[string]any, segments []string, constraintValue any, constrained bool, omitEmpty bool, omitIfEqualToSibling string) {
	keyPattern := segments[0]
	if !hasFieldSegmentWildcard(keyPattern) {
		if len(segments) == 1 {
			if shouldDeleteLeaf(current, keyPattern, constraintValue, constrained, omitEmpty, omitIfEqualToSibling) {
				delete(current, keyPattern)
			}

			return
		}

		next, ok := current[keyPattern]
		if !ok {
			return
		}

		deleteFieldPath(next, segments[1:], constraintValue, constrained, omitEmpty, omitIfEqualToSibling)

		return
	}

	for key, next := range current {
		if !matchGlob(keyPattern, key) {
			continue
		}

		if len(segments) == 1 {
			if shouldDeleteLeaf(current, key, constraintValue, constrained, omitEmpty, omitIfEqualToSibling) {
				delete(current, key)
			}

			continue
		}

		deleteFieldPath(next, segments[1:], constraintValue, constrained, omitEmpty, omitIfEqualToSibling)
	}
}

func deleteFieldPathFromStringMap(current map[string]string, segments []string, constraintValue any, constrained bool, omitEmpty bool) {
	if len(segments) != 1 {
		return
	}

	keyPattern := segments[0]
	if !hasFieldSegmentWildcard(keyPattern) {
		if shouldDeleteEntry(current[keyPattern], constraintValue, constrained, omitEmpty) {
			delete(current, keyPattern)
		}

		return
	}

	for key := range current {
		if matchGlob(keyPattern, key) && shouldDeleteEntry(current[key], constraintValue, constrained, omitEmpty) {
			delete(current, key)
		}
	}
}

func valueMatches(actual, expected any) bool {
	return fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected)
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

	for namespace := range strings.SplitSeq(namespacesCSV, ",") {
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

func parseKindFilter(kindFilterCSV string) ([]string, error) {
	trimmed := strings.TrimSpace(kindFilterCSV)
	if trimmed == "" {
		return nil, nil
	}

	patterns := make([]string, 0)

	for entry := range strings.SplitSeq(trimmed, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		err := validateGlobPattern(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid --kind glob %q: %w", entry, err)
		}

		patterns = append(patterns, entry)
	}

	if len(patterns) == 0 {
		return nil, fmt.Errorf("invalid --kind %q: no patterns found", kindFilterCSV)
	}

	return patterns, nil
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
	case item.GetKind() == kindNamespace && getGroup(item.GetAPIVersion()) == "":
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
	if gvk.Group != rbacGroup || gvk.Kind != kindClusterRole {
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
	if item.GroupVersionKind().Group != rbacGroup {
		return false
	}

	return item.GetLabels()[bootstrappingKey] == rbacDefaultsValue
}

// isDefaultServiceAccount reports whether the item is the `default`
// ServiceAccount that the service-account controller creates in every
// namespace.
func isDefaultServiceAccount(item *unstructured.Unstructured) bool {
	gvk := item.GroupVersionKind()
	return gvk.Group == "" && gvk.Kind == kindServiceAccount && item.GetName() == defaultName
}

// isKubeRootCAConfigMap reports whether the item is the `kube-root-ca.crt`
// ConfigMap that the root-ca-cert-publisher controller creates in every
// namespace.
func isKubeRootCAConfigMap(item *unstructured.Unstructured) bool {
	gvk := item.GroupVersionKind()
	return gvk.Group == "" && gvk.Kind == kindConfigMap && item.GetName() == kubeRootCAName
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

	if len(opts.kindFilter) > 0 && !matchesAnyGlob(opts.kindFilter, item.GetKind()) {
		return false
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

	if item.GetKind() == kindNamespace {
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
	if kind != kindSecret {
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
	metadata, ok := obj[fieldMetadata].(map[string]any)
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

func writeDumpMeta(outputDir, clusterHost string) error {
	m := dumpMeta{
		DumpedAt:    time.Now().UTC(),
		ClusterHost: clusterHost,
	}

	data, err := yaml.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal dump metadata: %w", err)
	}

	filePath := filepath.Join(outputDir, dumpMetaFileName)

	err = os.WriteFile(filePath, data, 0o600)
	if err != nil {
		return fmt.Errorf("failed to write dump metadata to %s: %w", filePath, err)
	}

	return nil
}

func readDumpMeta(dumpDir string) (dumpMeta, error) {
	filePath := filepath.Join(dumpDir, dumpMetaFileName)

	data, err := os.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return dumpMeta{}, fmt.Errorf("%q does not contain %s — was it created with a recent version of dumpall?", dumpDir, dumpMetaFileName)
		}

		return dumpMeta{}, fmt.Errorf("failed to read dump metadata from %s: %w", filePath, err)
	}

	var m dumpMeta

	err = yaml.Unmarshal(data, &m)
	if err != nil {
		return dumpMeta{}, fmt.Errorf("failed to parse dump metadata from %s: %w", filePath, err)
	}

	return m, nil
}

type topChurnEntry struct {
	RelPath     string
	GenA        int64
	GenB        int64
	Delta       int64
	RatePerHour float64
}

func buildTopChurnCmd() *cobra.Command {
	var (
		dumpWaitDump  string
		namespacesCSV string
		kindFilterCSV string
		removeDumps   bool
	)

	cmd := &cobra.Command{
		Use:          "top-churn [<dump-a> <dump-b>]",
		Short:        "Show resources with the highest generation increase rate between two dumps",
		Long:         "Compare two dump directories and list resources sorted by generation increase per hour.\nBoth dumps must have been created from the same cluster.\n\nWith --dump-wait-dump the command takes both dumps itself, waits between them, then compares.\nThe dump directories are kept on disk unless --remove is given.\nUse --namespaces and --kind to limit the scope of the automatic dumps (both accept comma-separated values).",
		SilenceUsage: true,
		Args:         cobra.RangeArgs(0, 2),
		RunE: func(_ *cobra.Command, args []string) error {
			if dumpWaitDump != "" {
				if len(args) != 0 {
					return fmt.Errorf("--dump-wait-dump takes no positional arguments")
				}

				wait, err := time.ParseDuration(dumpWaitDump)
				if err != nil {
					return fmt.Errorf("invalid duration %q: %w", dumpWaitDump, err)
				}

				return runTopChurnWithDumps(wait, namespacesCSV, kindFilterCSV, removeDumps)
			}

			if len(args) != 2 {
				return fmt.Errorf("provide two dump directories or use --dump-wait-dump")
			}

			return runTopChurn(args[0], args[1])
		},
	}

	cmd.Flags().StringVar(&dumpWaitDump, "dump-wait-dump", "", "Take two dumps with a wait between them, then compare (default wait: 7m)")
	cmd.Flag("dump-wait-dump").NoOptDefVal = "7m"
	cmd.Flags().BoolVar(&removeDumps, "remove", false, "Remove the dump directories after comparison (for --dump-wait-dump)")
	cmd.Flags().StringVarP(&namespacesCSV, "namespaces", "n", "", "Comma-separated list of namespaces to dump (for --dump-wait-dump)")
	cmd.Flags().StringVar(&kindFilterCSV, "kind", "", "Comma-separated list of kind globs to dump (for --dump-wait-dump, e.g. 'Deployment,StatefulSet')")

	return cmd
}

func runTopChurnWithDumps(wait time.Duration, namespacesCSV, kindFilterCSV string, removeDumps bool) error {
	dirA, err := os.MkdirTemp("", "dumpall-churn-a-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	dirB, err := os.MkdirTemp("", "dumpall-churn-b-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	if removeDumps {
		defer os.RemoveAll(dirA)
		defer os.RemoveAll(dirB)
	}

	buildDumpOpts := func(outputDir string) *options {
		return &options{
			outputDir:       outputDir,
			namespacesCSV:   namespacesCSV,
			kindFilterCSV:   kindFilterCSV,
			quiet:           true,
			overwriteOutdir: true,
		}
	}

	optsA := buildDumpOpts(dirA)

	err = finalizeOpts(optsA)
	if err != nil {
		return fmt.Errorf("configuring first dump: %w", err)
	}

	fmt.Printf("Taking first dump into %s...\n", dirA)

	err = runDump(optsA)
	if err != nil {
		return fmt.Errorf("first dump failed: %w", err)
	}

	fmt.Printf("Waiting %s...\n", wait)
	time.Sleep(wait)

	optsB := buildDumpOpts(dirB)

	err = finalizeOpts(optsB)
	if err != nil {
		return fmt.Errorf("configuring second dump: %w", err)
	}

	fmt.Printf("Taking second dump into %s...\n", dirB)

	err = runDump(optsB)
	if err != nil {
		return fmt.Errorf("second dump failed: %w", err)
	}

	return runTopChurn(dirA, dirB)
}

func runTopChurn(dumpA, dumpB string) error {
	metaA, err := readDumpMeta(dumpA)
	if err != nil {
		return fmt.Errorf("dump A: %w", err)
	}

	metaB, err := readDumpMeta(dumpB)
	if err != nil {
		return fmt.Errorf("dump B: %w", err)
	}

	if metaA.ClusterHost != metaB.ClusterHost {
		return fmt.Errorf("dumps are from different clusters: %q vs %q", metaA.ClusterHost, metaB.ClusterHost)
	}

	timeDelta := metaB.DumpedAt.Sub(metaA.DumpedAt)
	if timeDelta <= 0 {
		return fmt.Errorf("dump A (%s) is not earlier than dump B (%s); swap the arguments",
			metaA.DumpedAt.Format(time.RFC3339), metaB.DumpedAt.Format(time.RFC3339))
	}

	hours := timeDelta.Hours()

	filesA, err := findYAMLFiles(dumpA)
	if err != nil {
		return err
	}

	filesB, err := findYAMLFiles(dumpB)
	if err != nil {
		return err
	}

	relToAbsA := make(map[string]string, len(filesA))

	for _, f := range filesA {
		rel, err := filepath.Rel(dumpA, f)
		if err != nil {
			return fmt.Errorf("calculating relative path: %w", err)
		}

		relToAbsA[rel] = f
	}

	relToAbsB := make(map[string]string, len(filesB))

	for _, f := range filesB {
		rel, err := filepath.Rel(dumpB, f)
		if err != nil {
			return fmt.Errorf("calculating relative path: %w", err)
		}

		relToAbsB[rel] = f
	}

	var (
		entries      []topChurnEntry
		sameGenCount int
		anyGenFound  bool
		onlyInACount int
		onlyInBCount int
	)

	for rel := range relToAbsA {
		if _, inB := relToAbsB[rel]; !inB {
			onlyInACount++
		}
	}

	for rel := range relToAbsB {
		if _, inA := relToAbsA[rel]; !inA {
			onlyInBCount++
		}
	}

	for rel, absA := range relToAbsA {
		absB, ok := relToAbsB[rel]
		if !ok {
			continue
		}

		genA, err := readGeneration(absA)
		if err != nil {
			return err
		}

		genB, err := readGeneration(absB)
		if err != nil {
			return err
		}

		if genA != 0 || genB != 0 {
			anyGenFound = true
		}

		if genA == 0 && genB == 0 {
			continue
		}

		delta := genB - genA
		if delta <= 0 {
			sameGenCount++

			continue
		}

		entries = append(entries, topChurnEntry{
			RelPath:     strings.TrimSuffix(rel, ".yaml"),
			GenA:        genA,
			GenB:        genB,
			Delta:       delta,
			RatePerHour: float64(delta) / hours,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].RatePerHour > entries[j].RatePerHour
	})

	fmt.Printf("Dump A: %s  (%s)\n", dumpA, metaA.DumpedAt.Format(time.RFC3339))
	fmt.Printf("Dump B: %s  (%s)\n", dumpB, metaB.DumpedAt.Format(time.RFC3339))
	fmt.Printf("Cluster: %s\n", metaA.ClusterHost)
	fmt.Printf("Time delta: %s\n", timeDelta.Round(time.Second))
	fmt.Printf("Only in A: %d  Only in B: %d  Unchanged generation: %d\n\n", onlyInACount, onlyInBCount, sameGenCount)

	if len(entries) == 0 {
		fmt.Println("No generation increases found.")

		if !anyGenFound {
			fmt.Println("Hint: ensure both dumps were created without --ignore-config-use-common (which strips metadata.generation).")
		}

		return nil
	}

	maxLen := len("RESOURCE")

	for _, e := range entries {
		if len(e.RelPath) > maxLen {
			maxLen = len(e.RelPath)
		}
	}

	fmt.Printf("%-*s  %8s  %8s  %8s  %10s\n", maxLen, "RESOURCE", "GEN_A", "GEN_B", "DELTA", "RATE/h")

	for _, e := range entries {
		fmt.Printf("%-*s  %8d  %8d  %8d  %10.2f\n", maxLen, e.RelPath, e.GenA, e.GenB, e.Delta, e.RatePerHour)
	}

	return nil
}

func readGeneration(filePath string) (int64, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read %s: %w", filePath, err)
	}

	var obj struct {
		Metadata struct {
			Generation int64 `yaml:"generation"`
		} `yaml:"metadata"`
	}

	err = yaml.Unmarshal(data, &obj)
	if err != nil {
		return 0, fmt.Errorf("failed to parse YAML in %s: %w", filePath, err)
	}

	return obj.Metadata.Generation, nil
}
