#!/usr/bin/env bash
# Bash Strict Mode: https://github.com/guettli/bash-strict-mode
trap 'echo "Warning: A command has failed. Exiting the script. Line was ($0:$LINENO): $(sed -n "${LINENO}p" "$0")"; exit 3' ERR
set -Eeuo pipefail

direnv_dir="${DIRENV_DIR:-}"
direnv_dir="${direnv_dir#-}"
if [[ "${direnv_dir}" != "$(cd "$(dirname "$0")" && pwd)" ]]; then
	echo "DIRENV_DIR not set. Please execute like this:"
	echo "direnv exec . " "$@"
	exit 1
fi

find . -name '*.sh' -print0 | xargs -0 shellcheck

KIND_CLUSTER_NAME="dumpall-test"
TEST_NAMESPACE="dumpall-e2e"
TEST_SECRET_NAME="dumpall-secret"
TEST_SECRET_PLAINTEXT="dumpall-secret-value"
TEST_SECRET_BASE64=$(printf '%s' "$TEST_SECRET_PLAINTEXT" | base64)
TEST_NAME_REGEX="^${TEST_SECRET_NAME}$"
tmp_root=""
kind_kubeconfig=""

cleanup() {
	if [[ -n "${kind_kubeconfig}" ]]; then
		kind delete cluster --name "${KIND_CLUSTER_NAME}" --kubeconfig "${kind_kubeconfig}" || true
	fi

	if [[ -n "${tmp_root}" ]]; then
		rm -rf "${tmp_root}"
	fi
}

trap cleanup EXIT

require_binary() {
	local binary="$1"
	if ! command -v "${binary}" >/dev/null 2>&1; then
		echo "Missing required binary: ${binary}" >&2
		exit 1
	fi
}

assert_file_exists() {
	local path="$1"
	if [[ ! -f "${path}" ]]; then
		echo "Expected file does not exist: ${path}" >&2
		exit 1
	fi
}

assert_file_contains() {
	local path="$1"
	local expected="$2"
	if ! rg -F --quiet -- "${expected}" "${path}"; then
		echo "Expected file ${path} to contain: ${expected}" >&2
		exit 1
	fi
}

assert_file_not_contains() {
	local path="$1"
	local unexpected="$2"
	if rg -F --quiet -- "${unexpected}" "${path}"; then
		echo "Expected file ${path} to not contain: ${unexpected}" >&2
		exit 1
	fi
}

require_binary kind
require_binary kubectl
require_binary rg

go mod tidy

go test ./...

golangci-lint run --fix ./...

go run main.go gendocs

tmp_root="$(mktemp -d -t dumpall-e2e.XXXXXX)"
kind_kubeconfig="${tmp_root}/kubeconfig"

# Recreate cluster to make this integration test deterministic.
kind delete cluster --name "${KIND_CLUSTER_NAME}" --kubeconfig "${kind_kubeconfig}" || true
kind create cluster --name "${KIND_CLUSTER_NAME}" --kubeconfig "${kind_kubeconfig}"
export KUBECONFIG="${kind_kubeconfig}"

kubectl config use-context "kind-${KIND_CLUSTER_NAME}" >/dev/null

kubectl create namespace "${TEST_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic "${TEST_SECRET_NAME}" \
	--namespace "${TEST_NAMESPACE}" \
	--from-literal=password="${TEST_SECRET_PLAINTEXT}" \
	--dry-run=client -o yaml | kubectl apply -f -

redacted_outdir="${tmp_root}/redacted"
go run main.go --out-dir "${redacted_outdir}" --quiet

apiserver_dir="${redacted_outdir}/kube-system/Pod"
if [[ ! -d "${apiserver_dir}" ]]; then
	echo "Expected directory does not exist: ${apiserver_dir}" >&2
	exit 1
fi
apiserver_manifest="$(find "${apiserver_dir}" -maxdepth 1 -type f -name 'kube-apiserver-*.yaml' | head -n 1)"
if [[ -z "${apiserver_manifest}" ]]; then
	echo "Expected kube-apiserver pod manifest in ${apiserver_dir}" >&2
	exit 1
fi

redacted_secret_file="${redacted_outdir}/${TEST_NAMESPACE}/Secret/${TEST_SECRET_NAME}.yaml"
assert_file_exists "${redacted_secret_file}"
assert_file_contains "${redacted_secret_file}" "password: REDCATED-BY-DUMPALL"
assert_file_not_contains "${redacted_secret_file}" "${TEST_SECRET_BASE64}"
assert_file_not_contains "${redacted_secret_file}" "kubectl.kubernetes.io/last-applied-configuration"

raw_outdir="${tmp_root}/raw"
go run main.go --out-dir "${raw_outdir}" --quiet --dump-secrets
raw_secret_file="${raw_outdir}/${TEST_NAMESPACE}/Secret/${TEST_SECRET_NAME}.yaml"
assert_file_exists "${raw_secret_file}"
assert_file_contains "${raw_secret_file}" "password: ${TEST_SECRET_BASE64}"
assert_file_not_contains "${raw_secret_file}" "password: REDCATED-BY-DUMPALL"

filtered_outdir="${tmp_root}/filtered"
go run main.go --out-dir "${filtered_outdir}" --quiet --namespaces "${TEST_NAMESPACE}" --name-regex "${TEST_NAME_REGEX}"
filtered_secret_file="${filtered_outdir}/${TEST_NAMESPACE}/Secret/${TEST_SECRET_NAME}.yaml"
assert_file_exists "${filtered_secret_file}"
assert_file_contains "${filtered_secret_file}" "password: REDCATED-BY-DUMPALL"

if [[ -d "${filtered_outdir}/kube-system" ]]; then
	echo "Did not expect kube-system dump in namespace-filtered output" >&2
	exit 1
fi

filtered_file_count="$(find "${filtered_outdir}" -type f | wc -l | tr -d '[:space:]')"
if [[ "${filtered_file_count}" != "1" ]]; then
	echo "Expected exactly 1 dumped file for namespace+name filter, got ${filtered_file_count}" >&2
	find "${filtered_outdir}" -type f >&2
	exit 1
fi

git status
