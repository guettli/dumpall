package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	testAPIVersion       = "apiVersion"
	testKindField        = "kind"
	testNameField        = "name"
	testAnnotField       = "annotations"
	testLabelsField      = "labels"
	testSpecField        = "spec"
	testStatusField      = "status"
	testUIDField         = "uid"
	testControllerField  = "controller"
	rbacV1GroupVersion   = "rbac.authorization.k8s.io/v1"
	testAggRuleField     = "aggregationRule"
	testCRSelectorsField = "clusterRoleSelectors"
	testKindDeployment   = "Deployment"
	testFooPattern       = "foo-*"
	testNameDemo         = "demo"
	testLabelKeep        = "keep"
	testLabelRemove      = "remove"
	testProgressDLS      = "progressDeadlineSeconds"
	testAnnotRevision    = "deployment.kubernetes.io/revision"
	testAnnotClusterctl  = "clusterctl.cluster.x-k8s.io"
)

func TestWriteYAML_RedactsSecretDataAndRemovesLastAppliedAnnotation(t *testing.T) {
	t.Parallel()

	fixtureValue := "fixture-redaction-target-1"
	obj := secretObject(fixtureValue)

	outFile := filepath.Join(t.TempDir(), "secret.yaml")
	opts := &options{quiet: true}

	err := writeYAML(outFile, obj, opts)
	if err != nil {
		t.Fatalf("writeYAML returned error: %v", err)
	}

	contentBytes, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(contentBytes)

	if !strings.Contains(content, "password: "+redactionMarkerValue) {
		t.Fatalf("expected output to contain redacted password, got:\n%s", content)
	}

	if strings.Contains(content, fixtureValue) {
		t.Fatalf("output leaked secret value %q:\n%s", fixtureValue, content)
	}

	if strings.Contains(content, lastAppliedAnnotation) {
		t.Fatalf("output leaked annotation key %q:\n%s", lastAppliedAnnotation, content)
	}
}

func TestWriteYAML_DumpSecretsKeepsSecretValues(t *testing.T) {
	t.Parallel()

	fixtureValue := "fixture-redaction-target-2"
	obj := secretObject(fixtureValue)

	outFile := filepath.Join(t.TempDir(), "secret.yaml")
	opts := &options{
		quiet:       true,
		dumpSecrets: true,
	}

	err := writeYAML(outFile, obj, opts)
	if err != nil {
		t.Fatalf("writeYAML returned error: %v", err)
	}

	contentBytes, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(contentBytes)

	if !strings.Contains(content, "password: "+fixtureValue) {
		t.Fatalf("expected output to contain original password, got:\n%s", content)
	}

	if strings.Contains(content, redactionMarkerValue) {
		t.Fatalf("expected output to not contain redaction marker %q, got:\n%s", redactionMarkerValue, content)
	}
}

func TestWriteYAML_PrunesEmptyAnnotationsAfterRedaction(t *testing.T) {
	t.Parallel()

	fixtureValue := "fixture-redaction-target-3"
	obj := secretObject(fixtureValue)

	metadataMap, ok := obj[fieldMetadata].(map[string]any)
	if !ok {
		t.Fatal("expected metadata to be map[string]any")
	}

	annotations, ok := metadataMap[testAnnotField].(map[string]any)
	if !ok {
		t.Fatal("expected annotations to be map[string]any")
	}

	delete(annotations, "other")

	outFile := filepath.Join(t.TempDir(), "secret.yaml")
	opts := &options{quiet: true}

	err := writeYAML(outFile, obj, opts)
	if err != nil {
		t.Fatalf("writeYAML returned error: %v", err)
	}

	contentBytes, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(contentBytes)

	if strings.Contains(content, "annotations:") {
		t.Fatalf("expected empty annotations map to be pruned, got:\n%s", content)
	}
}

func TestPruneEmptyMetadataMaps_RemovesOnlyAnnotationsAndLabels(t *testing.T) {
	t.Parallel()

	obj := map[string]any{
		fieldMetadata: map[string]any{
			testAnnotField:  map[string]any{},
			testLabelsField: map[string]any{},
		},
		"webhooks": []any{
			map[string]any{
				"namespaceSelector": map[string]any{},
				fieldRules: []any{
					map[string]any{},
					map[string]any{
						"operations": []any{"CREATE"},
					},
				},
			},
		},
	}

	pruneEmptyMetadataMaps(obj)

	metadata, ok := obj[fieldMetadata].(map[string]any)
	if !ok {
		t.Fatal("expected metadata to be map[string]any")
	}

	if _, ok := metadata[testAnnotField]; ok {
		t.Fatalf("expected empty annotations map to be pruned")
	}

	if _, ok := metadata[testLabelsField]; ok {
		t.Fatalf("expected empty labels map to be pruned")
	}

	webhooks, ok := obj["webhooks"].([]any)
	if !ok {
		t.Fatal("expected webhooks to be []any")
	}

	webhook, ok := webhooks[0].(map[string]any)
	if !ok {
		t.Fatal("expected webhook to be map[string]any")
	}

	if _, ok := webhook["namespaceSelector"]; !ok {
		t.Fatalf("expected empty namespaceSelector map to be preserved; schema-blind pruning is unsafe because empty maps can be semantic Kubernetes values")
	}

	rules, ok := webhook[fieldRules].([]any)
	if !ok {
		t.Fatal("expected rules to be []any")
	}

	if len(rules) != 2 {
		t.Fatalf("expected empty map entries in lists to be preserved; schema-blind pruning is unsafe because empty maps can be semantic Kubernetes values")
	}
}

func TestWriteYAML_PreservesCRDStatusSubresources(t *testing.T) {
	t.Parallel()

	obj := map[string]any{
		testAPIVersion: "apiextensions.k8s.io/v1",
		testKindField:  "CustomResourceDefinition",
		fieldMetadata: map[string]any{
			testNameField: "extensionconfigs.runtime.cluster.x-k8s.io",
		},
		testSpecField: map[string]any{
			"group": "runtime.cluster.x-k8s.io",
			"names": map[string]any{
				testKindField: "ExtensionConfig",
				"plural":      "extensionconfigs",
				"singular":    "extensionconfig",
			},
			"scope": "Cluster",
			"versions": []any{
				map[string]any{
					testNameField: "v1alpha1",
					"served":      true,
					"storage":     false,
					"schema": map[string]any{
						"openAPIV3Schema": map[string]any{
							"type": "object",
						},
					},
					"subresources": map[string]any{
						testStatusField: map[string]any{},
					},
				},
				map[string]any{
					testNameField: "v1beta2",
					"served":      true,
					"storage":     true,
					"schema": map[string]any{
						"openAPIV3Schema": map[string]any{
							"type": "object",
						},
					},
					"subresources": map[string]any{
						testStatusField: map[string]any{},
					},
				},
			},
		},
	}

	outFile := filepath.Join(t.TempDir(), "crd.yaml")
	opts := &options{quiet: true}

	err := writeYAML(outFile, obj, opts)
	if err != nil {
		t.Fatalf("writeYAML returned error: %v", err)
	}

	contentBytes, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(contentBytes)
	if got := strings.Count(content, "status: {}"); got != 2 {
		t.Fatalf("expected both CRD versions to keep subresources.status, got %d occurrences:\n%s", got, content)
	}
}

func secretObject(fixtureValue string) map[string]any {
	return map[string]any{
		testAPIVersion: "v1",
		testKindField:  "Secret",
		fieldMetadata: map[string]any{
			testNameField: "my-secret",
			"namespace":   "default",
			testAnnotField: map[string]any{
				lastAppliedAnnotation: `{testAPIVersion:"v1",testKindField:"Secret","data":{"password":"` + fixtureValue + `"}}`,
				"other":               "keep-me",
			},
		},
		"data": map[string]any{
			"password": fixtureValue,
		},
	}
}

func TestParseNamespaceFilter(t *testing.T) {
	t.Parallel()

	filter, err := parseNamespaceFilter("alpha, beta ,alpha")
	if err != nil {
		t.Fatalf("parseNamespaceFilter returned error: %v", err)
	}

	if len(filter) != 2 {
		t.Fatalf("expected 2 namespaces, got %d", len(filter))
	}

	if _, ok := filter["alpha"]; !ok {
		t.Fatalf("expected alpha in filter")
	}

	if _, ok := filter["beta"]; !ok {
		t.Fatalf("expected beta in filter")
	}
}

func TestParseNamespaceFilter_InvalidOnlySeparators(t *testing.T) {
	t.Parallel()

	filter, err := parseNamespaceFilter(" , , ")
	if err == nil {
		t.Fatalf("expected error, got nil with filter %v", filter)
	}
}

func TestParseNameFilterRegex(t *testing.T) {
	t.Parallel()

	re, enabled, err := parseNameFilterRegex("^my-secret$")
	if err != nil {
		t.Fatalf("parseNameFilterRegex returned error: %v", err)
	}

	if !enabled {
		t.Fatalf("expected enabled=true")
	}

	if re == nil {
		t.Fatalf("expected regex, got nil")
	}

	if !re.MatchString("my-secret") {
		t.Fatalf("expected regex to match")
	}

	if re.MatchString("my-secret-2") {
		t.Fatalf("expected regex to not match")
	}
}

func TestParseNameFilterRegex_Invalid(t *testing.T) {
	t.Parallel()

	re, enabled, err := parseNameFilterRegex("[")
	if err == nil {
		t.Fatalf("expected error, got nil and regex %v enabled=%v", re, enabled)
	}
}

func TestParseNameFilterRegex_Empty(t *testing.T) {
	t.Parallel()

	re, enabled, err := parseNameFilterRegex("  ")
	if err != nil {
		t.Fatalf("parseNameFilterRegex returned error: %v", err)
	}

	if enabled {
		t.Fatalf("expected enabled=false")
	}

	if re == nil {
		t.Fatalf("expected non-nil regex for disabled state")
	}
}

func TestReadYamlFromDir_RecursiveAndAppliesIgnoreRules(t *testing.T) {
	t.Parallel()

	inputDir := filepath.Join(t.TempDir(), "input")

	err := os.MkdirAll(filepath.Join(inputDir, "nested"), 0o755)
	if err != nil {
		t.Fatalf("create input dir: %v", err)
	}

	err = os.WriteFile(filepath.Join(inputDir, "configmap.yaml"), []byte(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: cfg
  namespace: alpha
  ownerReferences:
    - apiVersion: v1
      kind: Namespace
      name: alpha
      uid: owner-1
data:
  key: value
status:
  phase: Active
`), 0o600)
	if err != nil {
		t.Fatalf("write configmap manifest: %v", err)
	}

	err = os.WriteFile(filepath.Join(inputDir, "nested", "namespace.yml"), []byte(`
apiVersion: v1
kind: Namespace
metadata:
  name: alpha
`), 0o600)
	if err != nil {
		t.Fatalf("write namespace manifest: %v", err)
	}

	err = os.WriteFile(filepath.Join(inputDir, "nested", "notes.txt"), []byte("ignored"), 0o600)
	if err != nil {
		t.Fatalf("write ignored file: %v", err)
	}

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - fields:
      - metadata.ownerReferences
      - status
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	outputDir := filepath.Join(t.TempDir(), "out")
	opts := &options{
		outputDir:   outputDir,
		quiet:       true,
		ignoreRules: rules,
	}

	err = readYamlFromDir(inputDir, opts)
	if err != nil {
		t.Fatalf("readYamlFromDir returned error: %v", err)
	}

	configMapOut := filepath.Join(outputDir, "alpha", "ConfigMap", "cfg.yaml")

	configMapBytes, err := os.ReadFile(configMapOut)
	if err != nil {
		t.Fatalf("read configmap output: %v", err)
	}

	configMapContent := string(configMapBytes)
	if strings.Contains(configMapContent, "ownerReferences:") {
		t.Fatalf("expected ownerReferences to be pruned, got:\n%s", configMapContent)
	}

	if strings.Contains(configMapContent, "status:") {
		t.Fatalf("expected status to be pruned, got:\n%s", configMapContent)
	}

	namespaceOut := filepath.Join(outputDir, clusterNamespace, "Namespace", "alpha.yaml")

	_, err = os.Stat(namespaceOut)
	if err != nil {
		t.Fatalf("expected namespace output file, got error: %v", err)
	}
}

func TestShouldProcessItem(t *testing.T) {
	t.Parallel()

	opts := &options{
		namespaceFilter: map[string]struct{}{
			"alpha": {},
		},
	}

	tests := []struct {
		name         string
		item         *unstructured.Unstructured
		isNamespaced bool
		want         bool
	}{
		{
			name:         "namespaced resource in filter",
			item:         newObject("ConfigMap", "cfg", "alpha"),
			isNamespaced: true,
			want:         true,
		},
		{
			name:         "namespaced resource not in filter",
			item:         newObject("ConfigMap", "cfg", "beta"),
			isNamespaced: true,
			want:         false,
		},
		{
			name:         "namespace object in filter",
			item:         newObject("Namespace", "alpha", ""),
			isNamespaced: false,
			want:         true,
		},
		{
			name:         "other cluster scoped resource skipped",
			item:         newObject("Node", "node-1", ""),
			isNamespaced: false,
			want:         false,
		},
	}

	for i := range tests {
		tc := tests[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := shouldProcessItem(tc.item, tc.isNamespaced, opts)
			if got != tc.want {
				t.Fatalf("shouldProcessItem() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShouldProcessItem_NameRegexAndNamespaceFilter(t *testing.T) {
	t.Parallel()

	opts := &options{
		namespaceFilter: map[string]struct{}{
			"alpha": {},
		},
		nameFilterEnabled: true,
		nameFilterRegex:   regexp.MustCompile(`^my-secret$`),
	}

	tests := []struct {
		name         string
		item         *unstructured.Unstructured
		isNamespaced bool
		want         bool
	}{
		{
			name:         "matches regex and namespace",
			item:         newObject("Secret", "my-secret", "alpha"),
			isNamespaced: true,
			want:         true,
		},
		{
			name:         "matches regex but wrong namespace",
			item:         newObject("Secret", "my-secret", "beta"),
			isNamespaced: true,
			want:         false,
		},
		{
			name:         "matches namespace but wrong name",
			item:         newObject("Secret", "other", "alpha"),
			isNamespaced: true,
			want:         false,
		},
	}

	for i := range tests {
		tc := tests[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := shouldProcessItem(tc.item, tc.isNamespaced, opts)
			if got != tc.want {
				t.Fatalf("shouldProcessItem() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShouldProcessItem_SkipOwned(t *testing.T) {
	t.Parallel()

	controllerTrue := true
	controllerFalse := false

	tests := []struct {
		name      string
		owners    []any
		skipOwned bool
		want      bool
	}{
		{
			name:      "no owner references, skipOwned on",
			owners:    nil,
			skipOwned: true,
			want:      true,
		},
		{
			name: "non-controlling owner reference, skipOwned on",
			owners: []any{
				map[string]any{
					testAPIVersion:      "v1",
					testKindField:       "Namespace",
					testNameField:       "default",
					testUIDField:        "uid-ns",
					testControllerField: controllerFalse,
				},
			},
			skipOwned: true,
			want:      true,
		},
		{
			name: "owner reference without controller field, skipOwned on",
			owners: []any{
				map[string]any{
					testAPIVersion: "v1",
					testKindField:  "Namespace",
					testNameField:  "default",
					testUIDField:   "uid-ns",
				},
			},
			skipOwned: true,
			want:      true,
		},
		{
			name: "controlling owner reference, skipOwned on",
			owners: []any{
				map[string]any{
					testAPIVersion:      "apps/v1",
					testKindField:       "ReplicaSet",
					testNameField:       "my-rs",
					testUIDField:        "uid-rs",
					testControllerField: controllerTrue,
				},
			},
			skipOwned: true,
			want:      false,
		},
		{
			name: "controlling owner reference, skipOwned off",
			owners: []any{
				map[string]any{
					testAPIVersion:      "apps/v1",
					testKindField:       "ReplicaSet",
					testNameField:       "my-rs",
					testUIDField:        "uid-rs",
					testControllerField: controllerTrue,
				},
			},
			skipOwned: false,
			want:      true,
		},
	}

	for i := range tests {
		tc := tests[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			item := newObject("Pod", "p", "default")
			if tc.owners != nil {
				metadataAny, ok := item.Object[fieldMetadata].(map[string]any)
				if !ok {
					t.Fatal("expected metadata to be map[string]any")
				}

				metadataAny["ownerReferences"] = tc.owners
			}

			opts := &options{skipOwned: tc.skipOwned}

			got := shouldProcessItem(item, true, opts)
			if got != tc.want {
				t.Fatalf("shouldProcessItem() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShouldProcessItem_SkipOwnedAutogenerated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		obj       map[string]any
		skipOwned bool
		want      bool
	}{
		{
			name: "aggregated ClusterRole, skipOwned on",
			obj: map[string]any{
				testAPIVersion: rbacV1GroupVersion,
				testKindField:  "ClusterRole",
				fieldMetadata:  map[string]any{testNameField: "admin"},
				testAggRuleField: map[string]any{
					testCRSelectorsField: []any{
						map[string]any{
							"matchLabels": map[string]any{
								"rbac.authorization.k8s.io/aggregate-to-admin": "true",
							},
						},
					},
				},
			},
			skipOwned: true,
			want:      false,
		},
		{
			name: "aggregated ClusterRole, skipOwned off",
			obj: map[string]any{
				testAPIVersion: rbacV1GroupVersion,
				testKindField:  "ClusterRole",
				fieldMetadata:  map[string]any{testNameField: "admin"},
				testAggRuleField: map[string]any{
					testCRSelectorsField: []any{},
				},
			},
			skipOwned: false,
			want:      true,
		},
		{
			name: "plain ClusterRole without aggregationRule, skipOwned on",
			obj: map[string]any{
				testAPIVersion: rbacV1GroupVersion,
				testKindField:  "ClusterRole",
				fieldMetadata:  map[string]any{testNameField: "view"},
				fieldRules:     []any{},
			},
			skipOwned: true,
			want:      true,
		},
		{
			name: "ClusterRole with empty aggregationRule map, skipOwned on",
			obj: map[string]any{
				testAPIVersion:   rbacV1GroupVersion,
				testKindField:    "ClusterRole",
				fieldMetadata:    map[string]any{testNameField: "view"},
				testAggRuleField: map[string]any{},
			},
			skipOwned: true,
			want:      true,
		},
		{
			name: "non-ClusterRole with aggregationRule field, skipOwned on",
			obj: map[string]any{
				testAPIVersion:   "v1",
				testKindField:    "ConfigMap",
				fieldMetadata:    map[string]any{testNameField: "cfg", "namespace": "default"},
				testAggRuleField: map[string]any{testCRSelectorsField: []any{}},
			},
			skipOwned: true,
			want:      true,
		},
		{
			name: "RBAC bootstrap ClusterRole, skipOwned on",
			obj: map[string]any{
				testAPIVersion: rbacV1GroupVersion,
				testKindField:  "ClusterRole",
				fieldMetadata: map[string]any{
					testNameField: "system:basic-user",
					testLabelsField: map[string]any{
						"kubernetes.io/bootstrapping": "rbac-defaults",
					},
				},
			},
			skipOwned: true,
			want:      false,
		},
		{
			name: "RBAC bootstrap RoleBinding, skipOwned on",
			obj: map[string]any{
				testAPIVersion: rbacV1GroupVersion,
				testKindField:  "RoleBinding",
				fieldMetadata: map[string]any{
					testNameField: "system:controller:bootstrap-signer",
					"namespace":   "kube-public",
					testLabelsField: map[string]any{
						"kubernetes.io/bootstrapping": "rbac-defaults",
					},
				},
			},
			skipOwned: true,
			want:      false,
		},
		{
			name: "ConfigMap with bootstrap label is not RBAC, skipOwned on",
			obj: map[string]any{
				testAPIVersion: "v1",
				testKindField:  "ConfigMap",
				fieldMetadata: map[string]any{
					testNameField: "cfg",
					"namespace":   "default",
					testLabelsField: map[string]any{
						"kubernetes.io/bootstrapping": "rbac-defaults",
					},
				},
			},
			skipOwned: true,
			want:      true,
		},
		{
			name: "default ServiceAccount, skipOwned on",
			obj: map[string]any{
				testAPIVersion: "v1",
				testKindField:  "ServiceAccount",
				fieldMetadata:  map[string]any{testNameField: "default", "namespace": "default"},
			},
			skipOwned: true,
			want:      false,
		},
		{
			name: "non-default ServiceAccount, skipOwned on",
			obj: map[string]any{
				testAPIVersion: "v1",
				testKindField:  "ServiceAccount",
				fieldMetadata:  map[string]any{testNameField: "my-sa", "namespace": "default"},
			},
			skipOwned: true,
			want:      true,
		},
		{
			name: "kube-root-ca.crt ConfigMap, skipOwned on",
			obj: map[string]any{
				testAPIVersion: "v1",
				testKindField:  "ConfigMap",
				fieldMetadata:  map[string]any{testNameField: "kube-root-ca.crt", "namespace": "default"},
			},
			skipOwned: true,
			want:      false,
		},
		{
			name: "kube-root-ca.crt ConfigMap, skipOwned off",
			obj: map[string]any{
				testAPIVersion: "v1",
				testKindField:  "ConfigMap",
				fieldMetadata:  map[string]any{testNameField: "kube-root-ca.crt", "namespace": "default"},
			},
			skipOwned: false,
			want:      true,
		},
		{
			name: "non-core ConfigMap-named kube-root-ca.crt, skipOwned on",
			obj: map[string]any{
				testAPIVersion: "example.com/v1",
				testKindField:  "ConfigMap",
				fieldMetadata:  map[string]any{testNameField: "kube-root-ca.crt", "namespace": "default"},
			},
			skipOwned: true,
			want:      true,
		},
	}

	for i := range tests {
		tc := tests[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			item := &unstructured.Unstructured{Object: tc.obj}
			opts := &options{skipOwned: tc.skipOwned}

			isNamespaced := false
			if _, ok := tc.obj[fieldMetadata].(map[string]any)["namespace"]; ok {
				isNamespaced = true
			}

			got := shouldProcessItem(item, isNamespaced, opts)
			if got != tc.want {
				t.Fatalf("shouldProcessItem() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShouldProcessItem_SkipRules(t *testing.T) {
	t.Parallel()

	_, skips, _, err := parseIgnoreConfigBytes("test", []byte(`
skipResources:
  - kind: Namespace
    name: foo-*
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	opts := &options{skipRules: skips}

	tests := []struct {
		name         string
		item         *unstructured.Unstructured
		isNamespaced bool
		want         bool
	}{
		{
			name:         "namespace foo-bar matches and is skipped",
			item:         newObject("Namespace", "foo-bar", ""),
			isNamespaced: false,
			want:         false,
		},
		{
			name:         "namespace foo-bar-baz matches anchored prefix",
			item:         newObject("Namespace", "foo-bar-baz", ""),
			isNamespaced: false,
			want:         false,
		},
		{
			name:         "myfoo-bar does not match (glob anchors at start)",
			item:         newObject("Namespace", "myfoo-bar", ""),
			isNamespaced: false,
			want:         true,
		},
		{
			name:         "ConfigMap with foo-bar name is not skipped (kind mismatch)",
			item:         newObject("ConfigMap", "foo-bar", "default"),
			isNamespaced: true,
			want:         true,
		},
	}

	for i := range tests {
		tc := tests[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := shouldProcessItem(tc.item, tc.isNamespaced, opts)
			if got != tc.want {
				t.Fatalf("shouldProcessItem() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShouldProcessItem_SkipRuleNamespacePattern(t *testing.T) {
	t.Parallel()

	_, skips, _, err := parseIgnoreConfigBytes("test", []byte(`
skipResources:
  - namespace: test-*
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	opts := &options{skipRules: skips}

	if shouldProcessItem(newObject("ConfigMap", "cfg", "test-1"), true, opts) {
		t.Fatalf("expected ConfigMap in test-1 to be skipped")
	}

	if !shouldProcessItem(newObject("ConfigMap", "cfg", "prod"), true, opts) {
		t.Fatalf("expected ConfigMap in prod to be kept")
	}
}

func TestShouldProcessItem_SkipRuleClusterScopedNamespace(t *testing.T) {
	t.Parallel()

	_, skips, _, err := parseIgnoreConfigBytes("test", []byte(`
skipResources:
  - namespace: _cluster
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	opts := &options{skipRules: skips}

	if !shouldProcessItem(newObject("ConfigMap", "cfg", "default"), true, opts) {
		t.Fatalf("expected namespaced ConfigMap to be kept")
	}

	if shouldProcessItem(newObject("Namespace", "alpha", ""), false, opts) {
		t.Fatalf("expected cluster-scoped Namespace to be skipped via _cluster")
	}
}

func TestSkipRuleCoversGVR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		yaml     string
		group    string
		kind     string
		expected bool
	}{
		{
			name:     "kind-only rule covers entire GVR",
			yaml:     "skipResources:\n  - kind: Secret\n",
			group:    "",
			kind:     "Secret",
			expected: true,
		},
		{
			name:     "kind-only rule does not match other kinds",
			yaml:     "skipResources:\n  - kind: Secret\n",
			group:    "",
			kind:     "ConfigMap",
			expected: false,
		},
		{
			name:     "group+kind rule covers entire GVR",
			yaml:     "skipResources:\n  - group: apps\n    kind: Deployment\n",
			group:    "apps",
			kind:     testKindDeployment,
			expected: true,
		},
		{
			name:     "kind+namespace rule does not cover entire GVR (still need LIST)",
			yaml:     "skipResources:\n  - kind: Secret\n    namespace: kube-*\n",
			group:    "",
			kind:     "Secret",
			expected: false,
		},
		{
			name:     "kind+name rule does not cover entire GVR (still need LIST)",
			yaml:     "skipResources:\n  - kind: Secret\n    name: foo-*\n",
			group:    "",
			kind:     "Secret",
			expected: false,
		},
		{
			name:     "kind glob covers matching kinds",
			yaml:     "skipResources:\n  - kind: \"*Role\"\n",
			group:    "rbac.authorization.k8s.io",
			kind:     "ClusterRole",
			expected: true,
		},
		{
			name:     "kind glob does not match unrelated kinds",
			yaml:     "skipResources:\n  - kind: \"*Role\"\n",
			group:    "",
			kind:     "ConfigMap",
			expected: false,
		},
	}

	for i := range tests {
		tc := tests[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, skips, _, err := parseIgnoreConfigBytes("test", []byte(tc.yaml))
			if err != nil {
				t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
			}

			got := skipRuleCoversGVR(skips, tc.group, tc.kind)
			if got != tc.expected {
				t.Fatalf("skipRuleCoversGVR(group=%q, kind=%q) = %v, want %v", tc.group, tc.kind, got, tc.expected)
			}
		})
	}
}

func TestCompileSkipRule_RejectsEmptyRule(t *testing.T) {
	t.Parallel()

	_, err := compileSkipRule(skipRuleFile{})
	if err == nil {
		t.Fatalf("expected error for skip rule with no matchers")
	}
}

func TestCompileSkipRule_InvalidGlob(t *testing.T) {
	t.Parallel()

	bad := "["

	_, err := compileSkipRule(skipRuleFile{Name: &bad})
	if err == nil {
		t.Fatalf("expected error for invalid glob")
	}
}

func TestParseIgnoreConfigBytes_SkipAndRulesCombined(t *testing.T) {
	t.Parallel()

	rules, skips, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - kind: ConfigMap
    fields:
      - status
skipResources:
  - kind: Namespace
    name: foo-*
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	if len(rules) != 1 {
		t.Fatalf("expected 1 ignore rule, got %d", len(rules))
	}

	if len(skips) != 1 {
		t.Fatalf("expected 1 skip rule, got %d", len(skips))
	}
}

func TestShouldProcessItem_ExcludeNamespaceDropsContents(t *testing.T) {
	t.Parallel()

	opts := &options{excludeNamespaces: []string{testFooPattern}}

	if shouldProcessItem(newObject("ConfigMap", "cfg", "foo-bar"), true, opts) {
		t.Fatalf("expected ConfigMap in foo-bar to be excluded")
	}

	if !shouldProcessItem(newObject("ConfigMap", "cfg", "kube-system"), true, opts) {
		t.Fatalf("expected ConfigMap in kube-system to be kept")
	}
}

func TestShouldProcessItem_ExcludeNamespaceDropsNamespaceObject(t *testing.T) {
	t.Parallel()

	opts := &options{excludeNamespaces: []string{testFooPattern}}

	if shouldProcessItem(newObject("Namespace", "foo-bar", ""), false, opts) {
		t.Fatalf("expected the foo-bar Namespace object to be excluded")
	}

	if !shouldProcessItem(newObject("Namespace", "kube-system", ""), false, opts) {
		t.Fatalf("expected non-matching Namespace to be kept")
	}
}

func TestShouldProcessItem_ExcludeNamespaceLeavesOtherClusterScopedAlone(t *testing.T) {
	t.Parallel()

	opts := &options{excludeNamespaces: []string{testFooPattern}}

	// Cluster-scoped resources other than Namespace must not be filtered by name
	// against excludeNamespaces — only Namespace objects are matched by name.
	if !shouldProcessItem(newObject("Node", "foo-1", ""), false, opts) {
		t.Fatalf("expected Node foo-1 to be kept (only Namespace objects match by name)")
	}
}

func TestParseIgnoreConfigBytes_ExcludeNamespacesParsed(t *testing.T) {
	t.Parallel()

	_, _, excludes, err := parseIgnoreConfigBytes("test", []byte(`
excludeNamespaces:
  - foo-*
  - test-*
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	if len(excludes) != 2 || excludes[0] != testFooPattern || excludes[1] != "test-*" {
		t.Fatalf("unexpected excludes: %v", excludes)
	}
}

func TestParseIgnoreConfigBytes_ExcludeNamespacesEmptyEntryRejected(t *testing.T) {
	t.Parallel()

	ignoreRules, skipRules, excludes, err := parseIgnoreConfigBytes("test", []byte(`
excludeNamespaces:
  - ""
`))
	_ = ignoreRules
	_ = skipRules
	_ = excludes

	if err == nil {
		t.Fatalf("expected error for empty pattern")
	}
}

func TestParseExcludeNamespacesCSV(t *testing.T) {
	t.Parallel()

	patterns, err := parseExcludeNamespacesCSV(" foo-* , test-* ,")
	if err != nil {
		t.Fatalf("parseExcludeNamespacesCSV returned error: %v", err)
	}

	if len(patterns) != 2 || patterns[0] != testFooPattern || patterns[1] != "test-*" {
		t.Fatalf("unexpected patterns: %v", patterns)
	}
}

func TestParseExcludeNamespacesCSV_EmptyReturnsNil(t *testing.T) {
	t.Parallel()

	patterns, err := parseExcludeNamespacesCSV("  ")
	if err != nil {
		t.Fatalf("parseExcludeNamespacesCSV returned error: %v", err)
	}

	if patterns != nil {
		t.Fatalf("expected nil patterns, got %v", patterns)
	}
}

func TestParseExcludeNamespacesCSV_OnlySeparatorsRejected(t *testing.T) {
	t.Parallel()

	_, err := parseExcludeNamespacesCSV(",,,")
	if err == nil {
		t.Fatalf("expected error for all-separator input")
	}
}

func TestParseIgnoreConfigBytes_LegacyRulesKeyRejected(t *testing.T) {
	t.Parallel()

	ignoreRules, skipRules, excludes, err := parseIgnoreConfigBytes("test", []byte(`
rules:
  - kind: ConfigMap
    fields:
      - status
`))
	_ = ignoreRules
	_ = skipRules
	_ = excludes

	if err == nil {
		t.Fatalf("expected error for legacy 'rules' key")
	}

	if !strings.Contains(err.Error(), "removeFields") {
		t.Fatalf("expected error message to suggest 'removeFields', got: %v", err)
	}
}

func TestParseIgnoreConfigBytes_LegacySkipKeyRejected(t *testing.T) {
	t.Parallel()

	ignoreRules, skipRules, excludes, err := parseIgnoreConfigBytes("test", []byte(`
skip:
  - kind: Namespace
    name: foo-*
`))
	_ = ignoreRules
	_ = skipRules
	_ = excludes

	if err == nil {
		t.Fatalf("expected error for legacy 'skip' key")
	}

	if !strings.Contains(err.Error(), "skipResources") {
		t.Fatalf("expected error message to suggest 'skipResources', got: %v", err)
	}
}

func TestParseIgnoreConfigBytes_StrictUnknownField(t *testing.T) {
	t.Parallel()

	ignoreRules, skipRules, excludes, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - kind: ConfigMap
    unexpected: true
    fields:
      - status
`))
	_ = ignoreRules
	_ = skipRules
	_ = excludes

	if err == nil {
		t.Fatalf("expected strict parsing error, got nil")
	}
}

func TestParseIgnoreConfigBytes_MissingMatchersBecomeWildcard(t *testing.T) {
	t.Parallel()

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - fields:
      - status
  - group: ""
    kind: Namespace
    fields:
      - spec.finalizers
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	if len(rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(rules))
	}

	if rules[0].GroupPattern != "*" || rules[0].KindPattern != "*" || rules[0].NamespacePattern != "*" || rules[0].NamePattern != "*" {
		t.Fatalf("expected missing matchers to default to *, got %+v", rules[0])
	}

	if rules[1].GroupPattern != "" {
		t.Fatalf("expected explicit empty group pattern to stay empty, got %q", rules[1].GroupPattern)
	}
}

func TestApplyIgnoreRules_GlobsEscapesAndLists(t *testing.T) {
	t.Parallel()

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - group: admissionregistration.k8s.io
    kind: "*WebhookConfiguration"
    name: capi-*
    fields:
      - metadata...kubectl\.kubernetes\.io/last-applied-configuration
      - webhooks...caBundle
      - webhooks...scope
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	obj := webhookObject()
	opts := &options{ignoreRules: rules}

	applyIgnoreRules(obj, opts)

	metadata, ok := obj[fieldMetadata].(map[string]any)
	if !ok {
		t.Fatal("expected metadata to be map[string]any")
	}

	annotations, ok := metadata[testAnnotField].(map[string]any)
	if !ok {
		t.Fatal("expected annotations to be map[string]any")
	}

	if _, ok := annotations[lastAppliedAnnotation]; ok {
		t.Fatalf("expected last-applied annotation to be removed")
	}

	webhooks, ok := obj["webhooks"].([]any)
	if !ok {
		t.Fatal("expected webhooks to be []any")
	}

	webhook, ok := webhooks[0].(map[string]any)
	if !ok {
		t.Fatal("expected webhook to be map[string]any")
	}

	clientConfig, ok := webhook["clientConfig"].(map[string]any)
	if !ok {
		t.Fatal("expected clientConfig to be map[string]any")
	}

	if _, ok := clientConfig["caBundle"]; ok {
		t.Fatalf("expected caBundle to be removed")
	}

	rulesList, ok := webhook[fieldRules].([]any)
	if !ok {
		t.Fatal("expected rules to be []any")
	}

	rule, ok := rulesList[0].(map[string]any)
	if !ok {
		t.Fatal("expected rule to be map[string]any")
	}

	if _, ok := rule["scope"]; ok {
		t.Fatalf("expected scope to be removed from each webhook rule")
	}
}

func TestApplyIgnoreRules_FieldSegmentGlobMatchesMapKeys(t *testing.T) {
	t.Parallel()

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - fields:
      - metadata.labels.kapp\.k14s\.io/*
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	obj := map[string]any{
		testAPIVersion: "v1",
		testKindField:  "ConfigMap",
		fieldMetadata: map[string]any{
			testNameField: testNameDemo,
			"namespace":   "default",
			testLabelsField: map[string]any{
				"app":                      testLabelKeep,
				"kapp.k14s.io/app":         testLabelRemove,
				"kapp.k14s.io/association": testLabelRemove,
			},
		},
	}

	applyIgnoreRules(obj, &options{ignoreRules: rules})

	metadataMap, ok := obj[fieldMetadata].(map[string]any)
	if !ok {
		t.Fatal("expected metadata to be map[string]any")
	}

	labels, ok := metadataMap[testLabelsField].(map[string]any)
	if !ok {
		t.Fatal("expected labels to be map[string]any")
	}

	if _, ok := labels["kapp.k14s.io/app"]; ok {
		t.Fatalf("expected kapp.k14s.io/app label to be removed")
	}

	if _, ok := labels["kapp.k14s.io/association"]; ok {
		t.Fatalf("expected kapp.k14s.io/association label to be removed")
	}

	if labels["app"] != testLabelKeep {
		t.Fatalf("expected non-matching label to remain, got %#v", labels["app"])
	}
}

func TestApplyIgnoreRules_StatusRuleDoesNotRemoveNestedStatus(t *testing.T) {
	t.Parallel()

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - fields:
      - status
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	obj := map[string]any{
		testAPIVersion: "v1",
		testKindField:  "ConfigMap",
		fieldMetadata: map[string]any{
			testNameField: testNameDemo,
			"namespace":   "default",
		},
		"foo": map[string]any{
			testStatusField: testLabelKeep,
		},
		"foo.status": "keep-literal-key",
		testStatusField: map[string]any{
			"phase": testLabelRemove,
		},
	}

	applyIgnoreRules(obj, &options{ignoreRules: rules})

	if _, ok := obj[testStatusField]; ok {
		t.Fatalf("expected top-level status to be removed")
	}

	foo, ok := obj["foo"].(map[string]any)
	if !ok {
		t.Fatal("expected foo to be map[string]any")
	}

	if foo[testStatusField] != testLabelKeep {
		t.Fatalf("expected nested foo.status to remain, got %#v", foo[testStatusField])
	}

	if obj["foo.status"] != "keep-literal-key" {
		t.Fatalf("expected literal foo.status key to remain, got %#v", obj["foo.status"])
	}
}

func TestApplyIgnoreRules_ValueConstraint_MatchingValueDeletes(t *testing.T) {
	t.Parallel()

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - kind: Deployment
    fields:
      - path: spec.progressDeadlineSeconds
        value: 600
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	obj := map[string]any{
		testAPIVersion: "apps/v1",
		testKindField:  testKindDeployment,
		fieldMetadata:  map[string]any{testNameField: testNameDemo, "namespace": "default"},
		testSpecField:  map[string]any{testProgressDLS: float64(600)},
	}

	applyIgnoreRules(obj, &options{ignoreRules: rules})

	spec, ok := obj[testSpecField].(map[string]any)
	if !ok {
		t.Fatal("expected spec to be map[string]any")
	}

	if _, ok := spec[testProgressDLS]; ok {
		t.Fatalf("expected progressDeadlineSeconds to be removed when value matches")
	}
}

func TestApplyIgnoreRules_ValueConstraint_NonMatchingValueKeeps(t *testing.T) {
	t.Parallel()

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - kind: Deployment
    fields:
      - path: spec.progressDeadlineSeconds
        value: 600
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	obj := map[string]any{
		testAPIVersion: "apps/v1",
		testKindField:  testKindDeployment,
		fieldMetadata:  map[string]any{testNameField: testNameDemo, "namespace": "default"},
		testSpecField:  map[string]any{testProgressDLS: float64(120)},
	}

	applyIgnoreRules(obj, &options{ignoreRules: rules})

	spec, ok := obj[testSpecField].(map[string]any)
	if !ok {
		t.Fatal("expected spec to be map[string]any")
	}

	if spec[testProgressDLS] != float64(120) {
		t.Fatalf("expected progressDeadlineSeconds to remain when value does not match, got %#v", spec[testProgressDLS])
	}
}

func TestApplyIgnoreRules_ValueConstraint_MixedFieldsInSameRule(t *testing.T) {
	t.Parallel()

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - kind: Deployment
    fields:
      - metadata.generation
      - path: spec.progressDeadlineSeconds
        value: 600
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	obj := map[string]any{
		testAPIVersion: "apps/v1",
		testKindField:  testKindDeployment,
		fieldMetadata:  map[string]any{testNameField: testNameDemo, "namespace": "default", "generation": int64(3)},
		testSpecField:  map[string]any{testProgressDLS: float64(600), "replicas": float64(1)},
	}

	applyIgnoreRules(obj, &options{ignoreRules: rules})

	metadata, ok := obj[fieldMetadata].(map[string]any)
	if !ok {
		t.Fatal("expected metadata to be map[string]any")
	}

	if _, ok := metadata["generation"]; ok {
		t.Fatalf("expected generation (no value constraint) to be removed")
	}

	spec, ok := obj[testSpecField].(map[string]any)
	if !ok {
		t.Fatal("expected spec to be map[string]any")
	}

	if _, ok := spec[testProgressDLS]; ok {
		t.Fatalf("expected progressDeadlineSeconds to be removed when value matches")
	}

	if spec["replicas"] != float64(1) {
		t.Fatalf("expected replicas to remain untouched, got %#v", spec["replicas"])
	}
}

func TestParseIgnoreConfigBytes_ValueConstraint_UnknownKeyRejected(t *testing.T) {
	t.Parallel()

	ignoreRules, skipRules, excludes, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - fields:
      - path: spec.foo
        value: bar
        unknown: oops
`))
	_ = ignoreRules
	_ = skipRules
	_ = excludes

	if err == nil {
		t.Fatal("expected error for unknown key in field entry map")
	}
}

func TestApplyIgnoreRules_OmitEmpty_RemovesNilField(t *testing.T) {
	t.Parallel()

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - fields:
      - path: metadata.labels
        omitempty: true
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	obj := map[string]any{
		testAPIVersion: "v1",
		testKindField:  "ConfigMap",
		fieldMetadata: map[string]any{
			testNameField:   testNameDemo,
			"namespace":     "default",
			testLabelsField: nil,
		},
	}

	applyIgnoreRules(obj, &options{ignoreRules: rules})

	metadata, ok := obj[fieldMetadata].(map[string]any)
	if !ok {
		t.Fatal("expected metadata to be map[string]any")
	}

	if _, ok := metadata[testLabelsField]; ok {
		t.Fatalf("expected nil labels to be removed by omitempty rule")
	}
}

func TestApplyIgnoreRules_OmitEmpty_RemovesEmptyMap(t *testing.T) {
	t.Parallel()

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - fields:
      - path: metadata.labels
        omitempty: true
      - path: metadata.annotations
        omitempty: true
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	obj := map[string]any{
		testAPIVersion: "v1",
		testKindField:  "ConfigMap",
		fieldMetadata: map[string]any{
			testNameField:   testNameDemo,
			"namespace":     "default",
			testLabelsField: map[string]any{},
			testAnnotField:  map[string]any{},
		},
	}

	applyIgnoreRules(obj, &options{ignoreRules: rules})

	metadata, ok := obj[fieldMetadata].(map[string]any)
	if !ok {
		t.Fatal("expected metadata to be map[string]any")
	}

	if _, ok := metadata[testLabelsField]; ok {
		t.Fatalf("expected empty labels map to be removed by omitempty rule")
	}

	if _, ok := metadata[testAnnotField]; ok {
		t.Fatalf("expected empty annotations map to be removed by omitempty rule")
	}
}

func TestApplyIgnoreRules_OmitEmpty_KeepsNonEmptyField(t *testing.T) {
	t.Parallel()

	rules, _, _, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - fields:
      - path: metadata.labels
        omitempty: true
`))
	if err != nil {
		t.Fatalf("parseIgnoreConfigBytes returned error: %v", err)
	}

	obj := map[string]any{
		testAPIVersion: "v1",
		testKindField:  "ConfigMap",
		fieldMetadata: map[string]any{
			testNameField:   testNameDemo,
			"namespace":     "default",
			testLabelsField: map[string]any{"app": "myapp"},
		},
	}

	applyIgnoreRules(obj, &options{ignoreRules: rules})

	metadata, ok := obj[fieldMetadata].(map[string]any)
	if !ok {
		t.Fatal("expected metadata to be map[string]any")
	}

	labels, ok := metadata[testLabelsField].(map[string]any)
	if !ok {
		t.Fatalf("expected non-empty labels to be preserved by omitempty rule")
	}

	if labels["app"] != "myapp" {
		t.Fatalf("expected labels to remain unchanged, got %#v", labels)
	}
}

func TestParseIgnoreConfigBytes_OmitEmpty_AndValueAreMutuallyExclusive(t *testing.T) {
	t.Parallel()

	ignoreRules, skipRules, excludes, err := parseIgnoreConfigBytes("test", []byte(`
removeFields:
  - fields:
      - path: metadata.labels
        value: null
        omitempty: true
`))
	_ = ignoreRules
	_ = skipRules
	_ = excludes

	if err == nil {
		t.Fatal("expected error when both 'value' and 'omitempty' are set in the same field entry")
	}
}

func TestWriteYAML_NoIgnoreRulesByDefault(t *testing.T) {
	t.Parallel()

	outFile := filepath.Join(t.TempDir(), "webhook.yaml")
	opts := &options{
		quiet:       true,
		ignoreRules: nil,
	}

	err := writeYAML(outFile, webhookObject(), opts)
	if err != nil {
		t.Fatalf("writeYAML returned error: %v", err)
	}

	contentBytes, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(contentBytes)

	for _, expected := range []string{
		"creationTimestamp:",
		"ownerReferences:",
		"resourceVersion:",
		"uid:",
		testAnnotRevision,
		testAnnotClusterctl,
		lastAppliedAnnotation,
		"caBundle:",
		"matchPolicy:",
		"reinvocationPolicy:",
		"timeoutSeconds:",
		"scope:",
		"status:",
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("expected output to contain %q, got:\n%s", expected, content)
		}
	}
}

func TestWriteYAML_CommonIgnoreConfigApplied(t *testing.T) {
	t.Parallel()

	rules, _, _, err := loadIgnoreRules(true, "")
	if err != nil {
		t.Fatalf("loadIgnoreRules returned error: %v", err)
	}

	outFile := filepath.Join(t.TempDir(), "webhook.yaml")
	opts := &options{
		quiet:       true,
		ignoreRules: rules,
	}

	err = writeYAML(outFile, webhookObject(), opts)
	if err != nil {
		t.Fatalf("writeYAML returned error: %v", err)
	}

	contentBytes, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(contentBytes)

	for _, unwanted := range []string{
		"annotations:",
		"creationTimestamp:",
		"ownerReferences:",
		"resourceVersion:",
		"uid:",
		testAnnotRevision,
		testAnnotClusterctl,
		lastAppliedAnnotation,
		"caBundle:",
		"matchPolicy:",
		"namespaceSelector:",
		"objectSelector:",
		"reinvocationPolicy:",
		"timeoutSeconds:",
		"scope:",
		"status:",
	} {
		if strings.Contains(content, unwanted) {
			t.Fatalf("expected output to not contain %q, got:\n%s", unwanted, content)
		}
	}
}

func TestWriteYAML_DumpManagedFieldsOverridesCommonIgnore(t *testing.T) {
	t.Parallel()

	rules, _, _, err := loadIgnoreRules(true, "")
	if err != nil {
		t.Fatalf("loadIgnoreRules returned error: %v", err)
	}

	obj := webhookObject()

	metadataMap, ok := obj[fieldMetadata].(map[string]any)
	if !ok {
		t.Fatal("expected metadata to be map[string]any")
	}

	metadataMap["managedFields"] = []any{
		map[string]any{"manager": "kubectl"},
	}

	outFile := filepath.Join(t.TempDir(), "webhook.yaml")
	opts := &options{
		quiet:             true,
		dumpManagedFields: true,
		ignoreRules:       rules,
	}

	err = writeYAML(outFile, obj, opts)
	if err != nil {
		t.Fatalf("writeYAML returned error: %v", err)
	}

	contentBytes, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	content := string(contentBytes)
	if !strings.Contains(content, "managedFields:") {
		t.Fatalf("expected managedFields to remain when dumpManagedFields=true, got:\n%s", content)
	}
}

func TestLoadIgnoreRules_DefaultIsEmpty(t *testing.T) {
	t.Parallel()

	rules, skips, _, err := loadIgnoreRules(false, "")
	if err != nil {
		t.Fatalf("loadIgnoreRules returned error: %v", err)
	}

	if len(rules) != 0 {
		t.Fatalf("expected no default ignore rules, got %d", len(rules))
	}

	if len(skips) != 0 {
		t.Fatalf("expected no default skip rules, got %d", len(skips))
	}
}

func TestLoadIgnoreRules_UserFileOnly(t *testing.T) {
	t.Parallel()

	configFile := filepath.Join(t.TempDir(), "ignore.yaml")

	err := os.WriteFile(configFile, []byte(`
removeFields:
  - kind: ConfigMap
    fields:
      - status
`), 0o600)
	if err != nil {
		t.Fatalf("write config file: %v", err)
	}

	rules, _, _, err := loadIgnoreRules(false, configFile)
	if err != nil {
		t.Fatalf("loadIgnoreRules returned error: %v", err)
	}

	if len(rules) != 1 {
		t.Fatalf("expected 1 user rule, got %d", len(rules))
	}
}

func TestLoadIgnoreRules_CommonAndUserFileAreCombined(t *testing.T) {
	t.Parallel()

	configFile := filepath.Join(t.TempDir(), "ignore.yaml")

	err := os.WriteFile(configFile, []byte(`
removeFields:
  - kind: ConfigMap
    fields:
      - status
`), 0o600)
	if err != nil {
		t.Fatalf("write config file: %v", err)
	}

	commonRules, _, _, err := loadIgnoreRules(true, "")
	if err != nil {
		t.Fatalf("loadIgnoreRules common returned error: %v", err)
	}

	rules, _, _, err := loadIgnoreRules(true, configFile)
	if err != nil {
		t.Fatalf("loadIgnoreRules combined returned error: %v", err)
	}

	if len(rules) != len(commonRules)+1 {
		t.Fatalf("expected %d combined rules, got %d", len(commonRules)+1, len(rules))
	}
}

func TestWriteCommonIgnoreConfig_PrintsEmbeddedYamlVerbatim(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	err := writeCommonIgnoreConfig(&buf)
	if err != nil {
		t.Fatalf("writeCommonIgnoreConfig returned error: %v", err)
	}

	if buf.String() != string(commonIgnoreConfig) {
		t.Fatalf("expected verbatim common ignore config output")
	}
}

func newObject(kind string, name string, namespace string) *unstructured.Unstructured {
	metadata := map[string]any{
		testNameField: name,
	}

	obj := map[string]any{
		testAPIVersion: "v1",
		testKindField:  kind,
		fieldMetadata:  metadata,
	}

	if namespace != "" {
		metadata["namespace"] = namespace
	}

	return &unstructured.Unstructured{Object: obj}
}

func webhookObject() map[string]any {
	return map[string]any{
		testAPIVersion: "admissionregistration.k8s.io/v1",
		testKindField:  "MutatingWebhookConfiguration",
		fieldMetadata: map[string]any{
			testNameField:       "capi-mutating-webhook-configuration",
			"creationTimestamp": "2026-04-22T00:00:00Z",
			"generation":        int64(2),
			"ownerReferences": []any{
				map[string]any{
					testAPIVersion: "v1",
					testKindField:  "Namespace",
					testNameField:  "capi-system",
					testUIDField:   "owner-uid-1",
				},
			},
			"resourceVersion": "123",
			testUIDField:      "uid-1",
			testAnnotField: map[string]any{
				testAnnotRevision:     "2",
				testAnnotClusterctl:   "",
				lastAppliedAnnotation: "present",
			},
		},
		"webhooks": []any{
			map[string]any{
				"clientConfig": map[string]any{
					"caBundle": "bundle",
					"service": map[string]any{
						testNameField: "webhook-service",
						"port":        int64(443),
					},
				},
				"matchPolicy":        "Equivalent",
				"namespaceSelector":  map[string]any{},
				"objectSelector":     map[string]any{},
				"reinvocationPolicy": "Never",
				fieldRules: []any{
					map[string]any{
						"scope": "*",
					},
				},
				"timeoutSeconds": int64(10),
			},
		},
		testStatusField: map[string]any{
			"observedGeneration": int64(2),
		},
	}
}

func TestValidateKindFilterMatchesResources(t *testing.T) {
	t.Parallel()

	resourceList := []*meta.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []meta.APIResource{
				{Kind: "ConfigMap"},
				{Kind: "Secret"},
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []meta.APIResource{
				{Kind: testKindDeployment},
			},
		},
	}

	tests := []struct {
		name       string
		kindFilter []string
		wantErr    bool
	}{
		{name: "empty filter passes", kindFilter: nil, wantErr: false},
		{name: "exact match passes", kindFilter: []string{"ConfigMap"}, wantErr: false},
		{name: "glob match passes", kindFilter: []string{"Config*"}, wantErr: false},
		{name: "multiple kinds one matches", kindFilter: []string{testKindDeployment, "DoesNotExist"}, wantErr: false},
		{name: "no match returns error", kindFilter: []string{"asdf"}, wantErr: true},
		{name: "all no match returns error", kindFilter: []string{"Foo", "Bar"}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateKindFilterMatchesResources(tc.kindFilter, resourceList)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}

			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
		})
	}
}
