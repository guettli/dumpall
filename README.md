# Dump all Kubernetes resources into a directory structure

Dumps all Kubernetes resources of a cluster into a directory structure:

```text
out/NAMESPACE/[GROUP_]KIND/NAME.yaml
```

For example:

```text
out/kube-system/ConfigMap/kubelet-config.yaml
out/kube-system/apps_Deployment/coredns.yaml
out/_cluster/Namespace/kube-system.yaml     (cluster-scoped resources)
```

The YAML is written like `kubectl get -o yaml` does.

dumpall talks to the api-server. The normalization (ignore rules, common
config, output layout) lives in [kubenorm](https://github.com/guettli/kubenorm),
which dumpall uses as a library. **If you only work with YAML files on disk**
(Helm output, CI checks, the
[Rendered Manifests Pattern](https://github.com/guettli/kubenorm#rendered-manifests-pattern)),
use kubenorm directly: it reads stdin, has `kubenorm norm --check`,
`kubenorm diff` and `kubenorm explain`, and does not pull in client-go.

## Install

With [mise](https://mise.jdx.dev), pinned to the latest release:

```terminal
mise use --pin github:guettli/dumpall@latest
```

This writes the exact version to `mise.toml`; `mise upgrade --bump
github:guettli/dumpall` moves to a newer release. mise skips releases younger
than 24 hours (`minimum_release_age`), so `@latest` can be one release behind
for a day. Alternatives: download a
binary from the [releases](https://github.com/guettli/dumpall/releases), or
`go run github.com/guettli/dumpall@latest` (needs Go).

Dump the cluster of your current kubeconfig context:

```terminal
dumpall

Written: out/cert-manager/Service/cert-manager.yaml
Written: out/cert-manager/Service/cert-manager-webhook.yaml
Written: out/default/Service/kubernetes.yaml
Written: out/_cluster/Namespace/cert-manager.yaml    <-- cluster-scoped resources use the directory "_cluster"
...
```

## Options

The resources of kind Secret are not dumped by default. If needed, use `--dump-secrets`.

To dump only specific namespaces, use `--namespaces` (or `-n`) with a comma-separated list, for example `--namespaces kube-system,default`.

To dump only resources whose `metadata.name` matches a regex, use `--name-regex` (or `-x`), for example `--name-regex '^kube-apiserver-.*'`.

To skip resources whose `metadata.name` matches a glob, use `--skip-name-glob`, for example `--skip-name-glob 'foo-*'` skips every resource whose name starts with `foo-`. The glob is anchored, so `foo-*` matches `foo-bar` but not `myfoo-bar`. For more selective skipping (by kind, group, or namespace too), use `skipResources` in the ignore config (see below).

To skip resources that are managed by a controller (i.e. resources with an entry in
`metadata.ownerReferences` where `controller: true`), use `--skip-owned` (or `-O`). For example,
Pods owned by a ReplicaSet, ReplicaSets owned by a Deployment, or Jobs owned by a CronJob will be
skipped. Resources with non-controlling owner references are still dumped.

`--skip-owned` also skips resources that Kubernetes autogenerates without an owner reference:

- ClusterRoles with an `aggregationRule` field (their `rules` are reconciled from other ClusterRoles).
- RBAC resources bootstrapped by the apiserver (those labeled `kubernetes.io/bootstrapping=rbac-defaults`).
- The `default` ServiceAccount that the service-account controller creates in every namespace.
- The `kube-root-ca.crt` ConfigMap that the root-ca-cert-publisher creates in every namespace.

Deprecated, will be removed: `--read-yaml-from` (use `kubenorm norm`),
`diff --read-yaml-from` (use `kubenorm diff`) and `check-normalized` (use
`kubenorm norm --check`). They still work and print a warning on stderr.

```terminal
# instead of: dumpall --read-yaml-from ./manifests --ignore-config-use-common -o ./normalized
kubenorm norm ./manifests -o ./normalized
```

To dump only a specific subset of resources from the api-server, use `--read-resource-names-from` with a file or directory of YAML manifests. dumpall reads the `kind`, `namespace`, and `name` of each resource in those files and dumps only the matching resources from the cluster. See [Pre-Deploy](#pre-deploy).

Ignore rules are optional and off by default:

1. No ignore flag: no ignore rules are applied.
2. `--ignore-config-use-common`: use the common config of kubenorm to reduce diff noise from server-managed fields and defaults.
3. `--ignore-config rules.yaml`: use only the rules from your YAML file.
4. `--ignore-config-use-common --ignore-config rules.yaml`: use both.

`dumpall show-common-ignore-config` prints the common config. The config format
(`removeFields`, `skipResources`, `excludeNamespaces`, `quantityFields`, ...)
is documented in the [kubenorm README](https://github.com/guettli/kubenorm#config).
Use `kubenorm explain` to see which rule changes or drops what.

dumpall reads the CRDs of the cluster, so quantities in custom resources are
canonicalized like in `kubenorm norm` (`1000m` becomes `"1"`). Without the
permission to list CRDs it warns and writes them as the api-server returns
them.

To exclude entire namespaces from the dump — including the `Namespace` object **and** every resource inside — use the `excludeNamespaces` key of the config, or the `--exclude-namespaces` flag:

```terminal
dumpall --exclude-namespaces 'foo-*,test-*'
```

Patterns are globs. Excluded namespaces are filtered by the api-server (field selector), so their resources are not transferred.

## Usage

<!-- usage:start -->
```text
Read resources from the api-server and dump each resource to a file.

The normalization is done by kubenorm (https://github.com/guettli/kubenorm).
For YAML files on disk (helm output, Rendered Manifests Pattern, CI checks)
use kubenorm directly.

Usage:
  dumpall [flags]
  dumpall [command]

Available Commands:
  check-normalized          Deprecated, use kubenorm norm --check
  completion                Generate the autocompletion script for the specified shell
  diff                      Diff the current cluster state against a local dump
  gendocs                   Generate usage.md from the command tree
  help                      Help about any command
  show-common-ignore-config Print the embedded common ignore config
  top-churn                 Show resources with the highest generation increase rate between two dumps
  version                   Print the version

Flags:
      --clean-out-dir                   Remove out-dir before dumping (only if it contains nothing but *.yaml files)
      --comment string                  Additional comment line to add at the top of each output YAML file
  -m, --dump-managed-fields             Dump managed fields (disabled by default)
  -s, --dump-secrets                    Dump secrets (disabled by default)
      --exclude-namespaces string       Comma-separated list of namespace globs to fully exclude (e.g. 'foo-*,test-*'). Drops the Namespace object plus all resources inside; uses fieldSelector to skip those namespaces api-server-side
  -h, --help                            help for dumpall
      --ignore-config file              Path to a YAML file with ignore rules
      --ignore-config-use-common        Use the embedded common ignore config
      --kind string                     Comma-separated list of kind globs to dump (e.g. 'ConfigMap,Secret,Cluster*')
  -x, --name-regex string               Only dump resources where metadata.name matches this regex
  -n, --namespaces string               Comma-separated list of namespaces to dump
  -o, --out-dir dir                     Output directory (must not exist) (default out)
  -q, --quiet                           Quiet, suppress output
      --read-resource-names-from path   Read resource identifiers (kind/namespace/name) from a YAML file or directory and dump only those resources. Useful to dump a specific subset of cluster resources from the api-server.
      --read-yaml-from path             Deprecated, use kubenorm norm. Read YAML manifests from a file or directory instead of connecting to the api-server.
      --secrets-as-stringdata           Write secret values as stringData (decoded from base64); requires --dump-secrets (-s)
      --skip-name-glob string           Skip resources where metadata.name matches this glob (e.g. 'foo-*' skips names starting with 'foo-')
  -O, --skip-owned                      Skip resources that have a controlling owner reference (e.g., Pods owned by a ReplicaSet) or that Kubernetes autogenerates from other resources (e.g., aggregated ClusterRoles)

Use "dumpall [command] --help" for more information about a command.
```
<!-- usage:end -->

For details on all subcommands and flags see [usage.md](usage.md).

## See Changes

After running dumpall you can modify your cluster, or just wait some time.

Then you can compare the changes with your favorite diff tool. I like [Meld](https://meldmerge.org/):

```terminal
mv out out-1

dumpall

meld out-1 out
```

## Pre-Deploy

You use the [Rendered Manifests Pattern](https://github.com/guettli/kubenorm#rendered-manifests-pattern):
your desired state is a tree of YAML files in `zz_rendered/`, and your
favorite GitOps tool (Flux, Argo CD, Rancher Fleet, ...) applies it.

Now you want to know: what will happen when these files get applied. You could
use `kubectl diff -R -f zz_rendered`, but this requires permission to PATCH
resources. In many setups only the GitOps tool has that permission. Additionally, it
shows many differences that are not real differences (like tracking labels of
the GitOps tool).

You can see a clean diff like this. Dump the current state of exactly the
resources in `zz_rendered`, normalized with the same common config:

```bash
dumpall --read-resource-names-from ./zz_rendered --ignore-config-use-common -o current-state
```

Compare it with the desired state. `kubenorm diff` normalizes both sides again
and pairs resources by identity, so the file layout of `zz_rendered` does not
matter. Install kubenorm like dumpall
(`mise use --pin github:guettli/kubenorm@latest`):

```bash
kubenorm diff current-state ./zz_rendered
```

Your CI can create this output for each pull request, so you directly see what
will happen if you apply the changes. In most cases this is identical to the
change of `zz_rendered/` in the pull request. But sometimes things are
different, especially if you modify `ignoreDifferences` of Argo CD.

## Related

- [kubenorm](https://github.com/guettli/kubenorm) Normalize Kubernetes YAML files on disk (Rendered Manifests Pattern). The normalization engine of dumpall.
- [konmon](https://github.com/guettli/konmon) Kubernetes Condition Monitor: watch all resources, keep a history in SQLite, alert via Alertmanager.
- [korphan](https://github.com/guettli/korphan) Find orphaned Kubernetes resources — those managed by neither a controller nor a GitOps tool.
- [Thomas WOL: Working out Loud](https://github.com/guettli/wol) Articles, projects, and insights spanning various topics in software development.

## Feedback is welcome

Please create an issue if you have a question or a feature request.
