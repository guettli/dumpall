package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

func secretObject(fixtureValue string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      "my-secret",
			"namespace": "default",
			"annotations": map[string]any{
				lastAppliedAnnotation: `{"apiVersion":"v1","kind":"Secret","data":{"password":"` + fixtureValue + `"}}`,
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

func TestParseIgnoreConfigBytes_StrictUnknownField(t *testing.T) {
	t.Parallel()

	_, err := parseIgnoreConfigBytes("test", []byte(`
rules:
  - kind: ConfigMap
    unexpected: true
    fields:
      - status
`))
	if err == nil {
		t.Fatalf("expected strict parsing error, got nil")
	}
}

func TestParseIgnoreConfigBytes_MissingMatchersBecomeWildcard(t *testing.T) {
	t.Parallel()

	rules, err := parseIgnoreConfigBytes("test", []byte(`
rules:
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

	rules, err := parseIgnoreConfigBytes("test", []byte(`
rules:
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

	metadata := obj["metadata"].(map[string]any)
	annotations := metadata["annotations"].(map[string]any)
	if _, ok := annotations[lastAppliedAnnotation]; ok {
		t.Fatalf("expected last-applied annotation to be removed")
	}

	webhooks := obj["webhooks"].([]any)
	webhook := webhooks[0].(map[string]any)
	clientConfig := webhook["clientConfig"].(map[string]any)
	if _, ok := clientConfig["caBundle"]; ok {
		t.Fatalf("expected caBundle to be removed")
	}

	rulesList := webhook["rules"].([]any)
	rule := rulesList[0].(map[string]any)
	if _, ok := rule["scope"]; ok {
		t.Fatalf("expected scope to be removed from each webhook rule")
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
		"resourceVersion:",
		"uid:",
		"clusterctl.cluster.x-k8s.io",
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
		if !strings.Contains(content, expected) {
			t.Fatalf("expected output to contain %q, got:\n%s", expected, content)
		}
	}
}

func TestWriteYAML_CommonIgnoreConfigApplied(t *testing.T) {
	t.Parallel()

	rules, err := loadIgnoreRules(true, "")
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
		"creationTimestamp:",
		"resourceVersion:",
		"uid:",
		"clusterctl.cluster.x-k8s.io",
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

	rules, err := loadIgnoreRules(true, "")
	if err != nil {
		t.Fatalf("loadIgnoreRules returned error: %v", err)
	}

	obj := webhookObject()
	obj["metadata"].(map[string]any)["managedFields"] = []any{
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

	rules, err := loadIgnoreRules(false, "")
	if err != nil {
		t.Fatalf("loadIgnoreRules returned error: %v", err)
	}

	if len(rules) != 0 {
		t.Fatalf("expected no default ignore rules, got %d", len(rules))
	}
}

func TestLoadIgnoreRules_UserFileOnly(t *testing.T) {
	t.Parallel()

	configFile := filepath.Join(t.TempDir(), "ignore.yaml")
	err := os.WriteFile(configFile, []byte(`
rules:
  - kind: ConfigMap
    fields:
      - status
`), 0o600)
	if err != nil {
		t.Fatalf("write config file: %v", err)
	}

	rules, err := loadIgnoreRules(false, configFile)
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
rules:
  - kind: ConfigMap
    fields:
      - status
`), 0o600)
	if err != nil {
		t.Fatalf("write config file: %v", err)
	}

	commonRules, err := loadIgnoreRules(true, "")
	if err != nil {
		t.Fatalf("loadIgnoreRules common returned error: %v", err)
	}

	rules, err := loadIgnoreRules(true, configFile)
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
		"name": name,
	}

	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       kind,
		"metadata":   metadata,
	}

	if namespace != "" {
		metadata["namespace"] = namespace
	}

	return &unstructured.Unstructured{Object: obj}
}

func webhookObject() map[string]any {
	return map[string]any{
		"apiVersion": "admissionregistration.k8s.io/v1",
		"kind":       "MutatingWebhookConfiguration",
		"metadata": map[string]any{
			"name":              "capi-mutating-webhook-configuration",
			"creationTimestamp": "2026-04-22T00:00:00Z",
			"generation":        int64(2),
			"resourceVersion":   "123",
			"uid":               "uid-1",
			"annotations": map[string]any{
				"clusterctl.cluster.x-k8s.io": "",
				lastAppliedAnnotation:         "present",
			},
		},
		"webhooks": []any{
			map[string]any{
				"clientConfig": map[string]any{
					"caBundle": "bundle",
					"service": map[string]any{
						"name": "webhook-service",
						"port": int64(443),
					},
				},
				"matchPolicy":        "Equivalent",
				"namespaceSelector":  map[string]any{},
				"objectSelector":     map[string]any{},
				"reinvocationPolicy": "Never",
				"rules": []any{
					map[string]any{
						"scope": "*",
					},
				},
				"timeoutSeconds": int64(10),
			},
		},
		"status": map[string]any{
			"observedGeneration": int64(2),
		},
	}
}
