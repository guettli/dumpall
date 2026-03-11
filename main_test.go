package main

import (
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
