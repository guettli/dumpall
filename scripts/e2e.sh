#!/usr/bin/env bash
# End-to-end test of dumpall against a real kube-apiserver. Needs no container
# runtime: etcd and kube-apiserver come from envtest (there is no kubelet, so
# no Pods).
#
# Besides the dump flags it checks the Pre-Deploy recipe of the README:
# applied manifests, dumped with --read-resource-names-from and the common
# config, must show no difference in `kubenorm diff` (kubenorm in the version
# of go.mod).
set -euo pipefail

k8s_version=${K8S_VERSION:-1.37.0}
repo=$(realpath "$(dirname "$0")/..")
manifests=$repo/scripts/testdata/files/e2e.yaml
work=$(mktemp -d)
etcd_pid='' apiserver_pid=''
cleanup() {
	# Stop the api-server before etcd, it waits for etcd while shutting down.
	[ -z "$apiserver_pid" ] || { kill "$apiserver_pid"; wait "$apiserver_pid"; } 2>/dev/null || true
	[ -z "$etcd_pid" ] || { kill "$etcd_pid"; wait "$etcd_pid"; } 2>/dev/null || true
	rm -rf "$work"
}
trap cleanup EXIT

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

bin=$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.25.2 use "$k8s_version" -p path)
go build -o "$work/dumpall" "$repo"
kubenorm_version=$(cd "$repo" && go list -m -f '{{.Version}}' github.com/guettli/kubenorm)
GOBIN=$work go install "github.com/guettli/kubenorm@$kubenorm_version"

openssl genrsa -out "$work/sa.key" 2048 2>/dev/null
echo "e2e-token,admin,admin,system:masters" >"$work/tokens.csv"

"$bin/etcd" --data-dir "$work/etcd" \
	--listen-client-urls http://127.0.0.1:23790 --advertise-client-urls http://127.0.0.1:23790 \
	--listen-peer-urls http://127.0.0.1:23800 >"$work/etcd.log" 2>&1 &
etcd_pid=$!

"$bin/kube-apiserver" --etcd-servers http://127.0.0.1:23790 \
	--bind-address 127.0.0.1 --secure-port 26443 --cert-dir "$work/certs" \
	--token-auth-file "$work/tokens.csv" --authorization-mode AlwaysAllow \
	--service-account-issuer https://kubernetes.default.svc \
	--service-account-key-file "$work/sa.key" --service-account-signing-key-file "$work/sa.key" \
	--service-cluster-ip-range 10.0.0.0/24 >"$work/apiserver.log" 2>&1 &
apiserver_pid=$!

export KUBECONFIG=$work/kubeconfig
cat >"$KUBECONFIG" <<-YAML
	apiVersion: v1
	kind: Config
	clusters: [{name: e2e, cluster: {server: "https://127.0.0.1:26443", insecure-skip-tls-verify: true}}]
	users: [{name: admin, user: {token: e2e-token}}]
	contexts: [{name: e2e, context: {cluster: e2e, user: admin}}]
	current-context: e2e
YAML
kubectl() { "$bin/kubectl" "$@"; }
dumpall() { "$work/dumpall" --quiet "$@"; }
kubenorm() { "$work/kubenorm" "$1" --no-config-discovery "${@:2}"; }

for _ in $(seq 60); do
	kubectl get --raw /readyz >/dev/null 2>&1 && break
	sleep 1
done
kubectl get --raw /readyz >/dev/null || { tail -20 "$work/apiserver.log"; exit 1; }

kubectl apply -f "$manifests" >/dev/null
kubectl create secret generic e2e-secret -n dumpall-e2e --from-literal=password=s3cret >/dev/null
secret_b64=$(printf s3cret | base64)

echo "--- secrets are skipped by default"
dumpall -o "$work/default"
[ ! -e "$work/default/dumpall-e2e/Secret" ] || fail "secrets dumped without --dump-secrets"
[ -f "$work/default/dumpall-e2e/ConfigMap/app-config.yaml" ] || fail "ConfigMap missing"

echo "--- --dump-secrets"
dumpall -o "$work/raw" --dump-secrets
grep -q "password: $secret_b64" "$work/raw/dumpall-e2e/Secret/e2e-secret.yaml" || fail "secret value missing"

echo "--- --namespaces and --name-regex"
dumpall -o "$work/filtered" --namespaces dumpall-e2e --name-regex '^app-config$'
count=$(find "$work/filtered" -type f ! -name _dumpall_meta.yaml | wc -l)
[ "$count" = 1 ] || { find "$work/filtered" -type f; fail "want 1 file, got $count"; }

echo "--- --kind"
dumpall -o "$work/kind" --kind ConfigMap --namespaces dumpall-e2e
other=$(find "$work/kind" -type f ! -name _dumpall_meta.yaml ! -path '*/ConfigMap/*')
[ -z "$other" ] || fail "--kind ConfigMap dumped: $other"

echo "--- --read-yaml-from is normalized like kubenorm norm (CRD quantities)"
dumpall -o "$work/files" --ignore-config-use-common --read-yaml-from "$repo/scripts/testdata/files"
grep -q 'size: 2Gi' "$work/files/dumpall-e2e/example.com_Widget/w.yaml" || fail "CRD quantity not canonical"
kubenorm norm --check "$work/files" || fail "--read-yaml-from output is not normalized"

echo "--- the common config output is normalized for kubenorm"
dumpall -o "$work/common" --ignore-config-use-common
kubenorm norm --check "$work/common" || fail "dumpall output is not normalized (kubenorm $kubenorm_version)"

echo "--- Pre-Deploy: cluster state equals the applied manifests"
dumpall -o "$work/current" --ignore-config-use-common --read-resource-names-from "$manifests"
kubenorm diff "$work/current" "$manifests" || fail "cluster state differs from $manifests (diff above)"

echo "ok (kubenorm $kubenorm_version)"
