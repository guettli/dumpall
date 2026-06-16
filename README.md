# Dump all Kubernetes resources into a directory structure

Dumps all Kubernetes resources into a directory structure:

```text
out/NAMESPACE/GVK/NAME.yaml
```

For example:

```text
out/kube-system/v1.ConfigMap/kubelet-config.yaml
```

The resources of kind Secret are not dumped by default. If needed, use `--dump-secrets`.

To dump only specific namespaces, use `--namespaces` (or `-n`) with a comma-separated list, for example `--namespaces kube-system,default`.

To dump only resources whose `metadata.name` matches a regex, use `--name-regex` (or `-x`), for example `--name-regex '^kube-apiserver-.*'`.

To skip resources whose `metadata.name` matches a glob, use `--skip-name-glob`, for example `--skip-name-glob 'foo-*'` skips every resource whose name starts with `foo-`. The glob is anchored, so `foo-*` matches `foo-bar` but not `myfoo-bar`. For more selective skipping (by kind, group, or namespace too), use the `skip:` section in the ignore config (see below).

To skip resources that are managed by a controller (i.e. resources with an entry in
`metadata.ownerReferences` where `controller: true`), use `--skip-owned` (or `-O`). For example,
Pods owned by a ReplicaSet, ReplicaSets owned by a Deployment, or Jobs owned by a CronJob will be
skipped. Resources with non-controlling owner references are still dumped.

`--skip-owned` also skips resources that Kubernetes autogenerates without an owner reference:

- ClusterRoles with an `aggregationRule` field (their `rules` are reconciled from other ClusterRoles).
- RBAC resources bootstrapped by the apiserver (those labeled `kubernetes.io/bootstrapping=rbac-defaults`).
- The `default` ServiceAccount that the service-account controller creates in every namespace.
- The `kube-root-ca.crt` ConfigMap that the root-ca-cert-publisher creates in every namespace.

To read existing YAML manifests instead of connecting to the api-server, use `--read-yaml-from` with a file or directory path. Directory input is recursive. This is useful for normalizing YAML files for better diffing — for example, stripping server-managed fields before committing manifests to git (see the [Rendered Manifests Pattern](https://akuity.io/blog/the-rendered-manifests-pattern)).

```terminal
# Normalize all manifests in a directory tree (strip noisy fields, then re-write)
dumpall --read-yaml-from ./manifests --ignore-config-use-common --out-dir ./normalized
```

To dump only a specific subset of resources from the api-server, use `--read-resource-names-from` with a file or directory of YAML manifests. dumpall reads the `kind`, `namespace`, and `name` of each resource in those files and dumps only the matching resources from the cluster. This pairs well with the [Rendered Manifests Pattern](https://akuity.io/blog/the-rendered-manifests-pattern):

```terminal
# Dump only the resources described in your rendered manifests directory
dumpall --read-resource-names-from ./rendered-manifests --out-dir ./cluster-state
```

Ignore rules are optional and off by default. There are three modes:

1. No ignore flag: output behaves like before, with no ignore rules applied.
2. `--ignore-config-use-common`: use the embedded common ignore config to reduce diff noise from server-managed fields.
3. `--ignore-config rules.yaml`: use only the rules from your YAML file.
4. `--ignore-config-use-common --ignore-config rules.yaml`: use the union of embedded common rules and your YAML rules.

Use `dumpall show-common-ignore-config` to print the embedded YAML config exactly as stored in the binary, including comments.

The YAML schema is parsed in strict mode, so unknown fields fail fast.

Example:

```yaml
removeFields:
  - group: apps
    kind: Deployment
    namespace: prod-*
    name: api-*
    fields:
      - status
      - metadata.annotations.example\.com/build-id
  - kind: Namespace
    name: org-*
    fields:
      - metadata...kubernetes\.io/metadata\.name
```

Each `removeFields` rule matches resources by `group`, `kind`, `namespace`, and `name` using globbing, then deletes the listed fields from every match. If one of those matchers is omitted, it behaves like `*`. Cluster-scoped resources use the namespace `_cluster` for matching. Fields use dot notation, literal dots in map keys must be escaped as `\.` and individual path segments can use `*` or `?` glob wildcards, for example `metadata.labels.kapp\.k14s\.io/*`. The token `...` means recursive descent across zero or more nested levels, and paths automatically walk lists, so `webhooks.clientConfig.caBundle` removes `caBundle` from every webhook entry.

The same config file can also list `skipResources` rules, which drop entire resources rather than just clearing fields:

```yaml
skipResources:
  - kind: Event
  - group: apps
    kind: Deployment
    namespace: test-*
    name: debug-*
```

The first rule drops every `Event` resource cluster-wide. The second drops Deployments named `debug-*` in any `test-*` namespace.

Each `skipResources` rule matches by `group`, `kind`, `namespace`, and `name` using the same globbing as `removeFields`, and **all** specified matchers must match for the resource to be skipped. Omitted matchers behave like `*`. Cluster-scoped resources use the namespace `_cluster`. A skip rule must set at least one matcher.

Note: a rule like `kind: Namespace, name: foo-*` only drops the cluster-scoped `Namespace` object itself — it does *not* drop resources living inside `foo-*` namespaces. To skip a namespace **and** everything inside it, use `excludeNamespaces` (see below).

To exclude entire namespaces from the dump — including the `Namespace` object **and** every resource inside — use the top-level `excludeNamespaces` key, or the `--exclude-namespaces` flag:

```yaml
excludeNamespaces:
  - foo-*
  - test-*
```

```terminal
dumpall --exclude-namespaces 'foo-*,test-*'
```

Patterns are globs and resolve against the live namespace list. When connected to an api-server, dumpall passes a `metadata.namespace!=...` field selector on every namespaced `List` call, so excluded namespaces are filtered server-side and their resources never cross the wire — much faster than filtering client-side when one excluded namespace is huge. In file/dir input mode the same patterns are applied client-side.

## Via `go run`

The easiest way is to run the code like this:

```terminal
go run github.com/guettli/dumpall@latest

Written: out/cert-manager/v1.Service/cert-manager.yaml
Written: out/cert-manager/v1.Service/cert-manager-webhook.yaml
Written: out/default/v1.Service/kubernetes.yaml
Written: out/_cluster/v1.Namespace/cert-manager.yaml    <-- non-namespaces resources use the directory "_cluster"
...
```

## Usage

<!-- usage:start -->
```text
Read resources from the api-server (or a YAML file/directory via --read-yaml-from) and dump each resource to a file.

Usage:
  dumpall [flags]
  dumpall [command]

Available Commands:
  check-normalized          Check whether a YAML file or directory is already normalized
  completion                Generate the autocompletion script for the specified shell
  diff                      Diff the current cluster state against a local dump
  gendocs                   Generate usage.md from the command tree
  help                      Help about any command
  show-common-ignore-config Print the embedded common ignore config
  top-churn                 Show resources with the highest generation increase rate between two dumps
  version                   Print the version

Flags:
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
      --read-yaml-from path             Read YAML manifests from a file or directory instead of connecting to the api-server. Useful for normalizing existing YAML files for better diffing.
  -r, --remove-out-dir                  Remove out-dir before dumping (disabled by default)
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

go run github.com/guettli/dumpall@latest

meld out-1 out
```

## Pre-Deploy

Imagine you use the Rendered Manifest Pattern, and you have created your desired state in a bunch of
YAML files in the directory called `generated`.

Now you want to know: what will happen when these files get applied. You could use `kubectl diff -R
-f generated`, but this requires permission to PATCH resources. In many setups only
ArgoCD has that permission. Additionally, it
will show many differences that are not real differences (like ArgoCD labels).

You can see a clean diff like this:

Generate a normalized version of your desired-state:

```bash
go run github.com/guettli/dumpall@latest --read-yaml-from ./generated --ignore-config-use-common -o desired-state
```

Generate a normalized version of the current state:

```bash
go run github.com/guettli/dumpall@latest --read-resource-names-from ./generated --ignore-config-use-common -o current-state
```

Now compare both directories with your favorite tool, like `diff -r` or `meld`:

```bash
diff -r current-state desired-state
```

I guess you use Pull-Requests for GitOps. You could make your CI create the above output for each
PR, so you directly see what will happen if you apply these changes. In most cases this will be
identical to the changes you see in the PR (directory `generated` is in git). But sometimes things
are different. Especially, if you modify `ignoreDifference` of ArgoCD.

## Related

- [check-conditions](https://github.com/guettli/check-conditions) Tiny tool to check all conditions of all resources in your Kubernetes cluster.
- [Thomas WOL: Working out Loud](https://github.com/guettli/wol) Articles, projects, and insights spanning various topics in software development.

## Feedback is welcome

Please create an issue if you have a question or a feature request.
