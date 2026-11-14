package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
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

	if strings.Contains(content, "kubectl.kubernetes.io/last-applied-configuration") {
		t.Fatalf("output leaked annotation key %q:\n%s", "kubectl.kubernetes.io/last-applied-configuration", content)
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

func TestWriteYAML_SecretsAsStringData(t *testing.T) {
	t.Parallel()

	plainValue := "my-plain-password"
	encodedValue := base64.StdEncoding.EncodeToString([]byte(plainValue))

	obj := map[string]any{
		testAPIVersion: "v1",
		testKindField:  "Secret",
		fieldMetadata: map[string]any{
			testNameField: "my-secret",
			"namespace":   "default",
		},
		"data": map[string]any{
			"password": encodedValue,
		},
	}

	outFile := filepath.Join(t.TempDir(), "secret.yaml")
	opts := &options{
		quiet:               true,
		dumpSecrets:         true,
		secretsAsStringData: true,
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

	if !strings.Contains(content, "stringData:") {
		t.Fatalf("expected output to contain stringData, got:\n%s", content)
	}

	if !strings.Contains(content, "password: "+plainValue) {
		t.Fatalf("expected output to contain decoded password %q, got:\n%s", plainValue, content)
	}

	if strings.Contains(content, "\ndata:\n") {
		t.Fatalf("expected output to not contain top-level 'data:' field, got:\n%s", content)
	}

	if strings.Contains(content, encodedValue) {
		t.Fatalf("expected output to not contain base64-encoded value %q, got:\n%s", encodedValue, content)
	}
}

func TestFinalizeOpts_SecretsAsStringDataRequiresDumpSecrets(t *testing.T) {
	t.Parallel()

	opts := &options{
		secretsAsStringData: true,
		dumpSecrets:         false,
	}

	err := finalizeOpts(opts)
	if err == nil {
		t.Fatal("expected error when --secrets-as-stringdata is set without --dump-secrets")
	}

	if !strings.Contains(err.Error(), "--dump-secrets") {
		t.Fatalf("expected error to mention --dump-secrets, got: %v", err)
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
				"kubectl.kubernetes.io/last-applied-configuration": `{testAPIVersion:"v1",testKindField:"Secret","data":{"password":"` + fixtureValue + `"}}`,
				"other": "keep-me",
			},
		},
		"data": map[string]any{
			"password": fixtureValue,
		},
	}
}

func TestReadYamlFromDir_SkipsDocumentsWithoutMetadata(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()

	err := os.WriteFile(filepath.Join(inputDir, "kustomization.yaml"), []byte(`
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
`), 0o600)
	if err != nil {
		t.Fatalf("write kustomization file: %v", err)
	}

	err = os.WriteFile(filepath.Join(inputDir, "configmap.yaml"), []byte(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: cfg
  namespace: default
data:
  key: value
`), 0o600)
	if err != nil {
		t.Fatalf("write configmap file: %v", err)
	}

	outputDir := filepath.Join(t.TempDir(), "out")
	opts := &options{
		outputDir: outputDir,
		quiet:     true,
	}

	err = readYamlFromDir(inputDir, opts)
	if err != nil {
		t.Fatalf("readYamlFromDir returned error: %v", err)
	}

	_, err = os.Stat(filepath.Join(outputDir, "default", "ConfigMap", "cfg.yaml"))
	if err != nil {
		t.Fatalf("expected configmap output file: %v", err)
	}

	kustomizeOut := filepath.Join(outputDir, clusterNamespace, "Kustomization")

	_, err = os.Stat(kustomizeOut)
	if err == nil {
		t.Fatalf("expected kustomization to be skipped, but found output at %s", kustomizeOut)
	}
}

func TestShouldProcessItem(t *testing.T) {
	t.Parallel()

	opts := &options{
		namespaceFilter: []string{"alpha"},
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
				"rules":        []any{},
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

func TestRunDiff_WithExistingTempDirs(t *testing.T) {
	t.Parallel()

	// Create temporary directories representing the two sides of the diff.
	dirA := t.TempDir()
	dirB := t.TempDir()

	// Create some sample YAML files in both directories.
	manifestA := filepath.Join(dirA, "configmap.yaml")
	manifestB := filepath.Join(dirB, "configmap.yaml")

	contentA := `apiVersion: v1
kind: ConfigMap
metadata:
  name: test-config
data:
  key: valueA
`
	contentB := `apiVersion: v1
kind: ConfigMap
metadata:
  name: test-config
data:
  key: valueB
`

	err := os.WriteFile(manifestA, []byte(contentA), 0o600)
	if err != nil {
		t.Fatalf("failed to write manifest A: %v", err)
	}

	err = os.WriteFile(manifestB, []byte(contentB), 0o600)
	if err != nil {
		t.Fatalf("failed to write manifest B: %v", err)
	}

	// runDiff should not fail with "output directory already exists" error.
	// Since the values for data.key differ, it should return errDiffsFound.
	err = runDiff([]string{"--read-yaml-from", dirA, dirB})
	if err == nil {
		t.Fatal("expected differences found error, got nil")
	}

	if !errors.Is(err, errDiffsFound) {
		t.Fatalf("expected errDiffsFound, got: %v", err)
	}
}

func TestRunDiff_QuietDefault(t *testing.T) {
	// Create temporary directories representing the two sides of the diff.
	dirA := t.TempDir()
	dirB := t.TempDir()

	manifestA := filepath.Join(dirA, "configmap.yaml")
	manifestB := filepath.Join(dirB, "configmap.yaml")

	content := `apiVersion: v1
kind: ConfigMap
metadata:
  name: test-config
data:
  key: value
`

	err := os.WriteFile(manifestA, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("failed to write manifest A: %v", err)
	}

	err = os.WriteFile(manifestB, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("failed to write manifest B: %v", err)
	}

	// Capture stdout.
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Run with quiet default (should NOT print progress outputs like "Written:")
	err = runDiff([]string{"--read-yaml-from", dirA, dirB})

	w.Close()

	var buf bytes.Buffer

	_, _ = io.Copy(&buf, r)
	os.Stdout = oldStdout

	if err != nil {
		t.Fatalf("expected no differences/errors, got: %v", err)
	}

	output := buf.String()
	if strings.Contains(output, "Written:") || strings.Contains(output, "Total files written:") {
		t.Errorf("expected quiet output by default, but got progress logs: %q", output)
	}

	// Run with --quiet=false (should print progress outputs like "Written:")
	r2, w2, _ := os.Pipe()
	os.Stdout = w2

	err = runDiff([]string{"--read-yaml-from", dirA, dirB, "--quiet=false"})

	w2.Close()

	var buf2 bytes.Buffer

	_, _ = io.Copy(&buf2, r2)
	os.Stdout = oldStdout

	if err != nil {
		t.Fatalf("expected no differences/errors, got: %v", err)
	}

	output2 := buf2.String()
	if !strings.Contains(output2, "Written:") || !strings.Contains(output2, "Total files written:") {
		t.Errorf("expected progress logs with --quiet=false, but got output: %q", output2)
	}
}

// The normalization engine is tested in github.com/guettli/kubenorm. The
// tests below check that dumpall's flags reach it.

func finalizedOpts(t *testing.T, opts *options) *options {
	t.Helper()

	err := finalizeOpts(opts)
	if err != nil {
		t.Fatalf("finalizeOpts: %v", err)
	}

	return opts
}

func TestWriteYAML_FlagsReachKubenorm(t *testing.T) {
	t.Parallel()

	newDeployment := func() map[string]any {
		return map[string]any{
			testAPIVersion: appsV1,
			testKindField:  testKindDeployment,
			"metadata": map[string]any{
				testNameField:   testNameDemo,
				"namespace":     "default",
				testUIDField:    "abc",
				"managedFields": []any{map[string]any{"manager": "kubectl"}},
			},
			testSpecField:   map[string]any{testProgressDLS: 600, "replicas": 1},
			testStatusField: map[string]any{"replicas": 1},
		}
	}

	for _, tc := range []struct {
		name        string
		opts        *options
		contains    []string
		notContains []string
	}{
		{
			name:        "no ignore config",
			opts:        &options{},
			contains:    []string{"uid: abc", "status:", testProgressDLS},
			notContains: []string{"managedFields"},
		},
		{
			name:        "common ignore config",
			opts:        &options{ignoreConfigUseCommon: true},
			contains:    []string{"replicas: 1"},
			notContains: []string{"uid:", "status:", testProgressDLS, "managedFields"},
		},
		{
			name:     "dump managed fields wins over common config",
			opts:     &options{ignoreConfigUseCommon: true, dumpManagedFields: true},
			contains: []string{"managedFields", "manager: kubectl"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			outFile := filepath.Join(t.TempDir(), "out.yaml")

			err := writeYAML(outFile, newDeployment(), finalizedOpts(t, tc.opts))
			if err != nil {
				t.Fatal(err)
			}

			data, err := os.ReadFile(outFile)
			if err != nil {
				t.Fatal(err)
			}

			content := string(data)
			if !strings.HasPrefix(content, "# https://github.com/guettli/dumpall version: ") {
				t.Fatalf("missing dumpall header:\n%s", content)
			}

			for _, want := range tc.contains {
				if !strings.Contains(content, want) {
					t.Errorf("missing %q in:\n%s", want, content)
				}
			}

			for _, unwanted := range tc.notContains {
				if strings.Contains(content, unwanted) {
					t.Errorf("unexpected %q in:\n%s", unwanted, content)
				}
			}
		})
	}
}

func TestShouldProcessItem_FilterFlags(t *testing.T) {
	t.Parallel()

	opts := finalizedOpts(t, &options{
		namespacesCSV:        "alpha,foo-1",
		nameRegex:            "^keep-",
		skipNameGlob:         "keep-skipped",
		excludeNamespacesCSV: testFooPattern,
	})

	for _, tc := range []struct {
		item         *unstructured.Unstructured
		isNamespaced bool
		want         bool
	}{
		{newObject("ConfigMap", "keep-a", "alpha"), true, true},
		{newObject("ConfigMap", "drop-a", "alpha"), true, false},
		{newObject("ConfigMap", "keep-a", "beta"), true, false},
		{newObject("ConfigMap", "keep-skipped", "alpha"), true, false},
		{newObject("ConfigMap", "keep-a", "foo-1"), true, false},
		{newObject("Namespace", "keep-ns", ""), false, false},
	} {
		got := shouldProcessItem(tc.item, tc.isNamespaced, opts)
		if got != tc.want {
			t.Errorf("%s %s/%s: got %v, want %v", tc.item.GetKind(), tc.item.GetNamespace(), tc.item.GetName(), got, tc.want)
		}
	}
}

func TestReadYamlFromDir_AppliesIgnoreConfigFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	inputDir := filepath.Join(dir, "input")
	configFile := filepath.Join(dir, "ignore.yaml")

	err := os.MkdirAll(filepath.Join(inputDir, "nested"), 0o755)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(filepath.Join(inputDir, "nested", "cm.yml"), []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cfg\n  namespace: alpha\ndata:\n  key: value\nstatus:\n  phase: Active\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(configFile, []byte("removeFields:\n  - fields:\n      - status\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	outputDir := filepath.Join(dir, "out")

	err = readYamlFromDir(inputDir, finalizedOpts(t, &options{outputDir: outputDir, quiet: true, ignoreConfigFile: configFile}))
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(outputDir, "alpha", "ConfigMap", "cfg.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(data), "status:") || !strings.Contains(string(data), "key: value") {
		t.Fatalf("unexpected output:\n%s", data)
	}
}
