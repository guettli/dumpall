package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
