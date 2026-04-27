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

To read existing YAML manifests instead of connecting to the api-server, use `--file-name` (or `-f`) for a single file or `--dir` (or `-d`) for a directory tree. Directory input is recursive and useful if you want to normalize existing YAML files with ignore rules.

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

[Usage](https://github.com/guettli/dumpall/blob/main/usage.md)

## See Changes

After running dumpall you can modify your cluster, or just wait some time.

Then you can compare the changes with your favorite diff tool. I like [Meld](https://meldmerge.org/):

```terminal
mv out out-1

go run github.com/guettli/dumpall@latest

meld out-1 out
```

## Related

* [check-conditions](https://github.com/guettli/check-conditions) Tiny tool to check all conditions of all resources in your Kubernetes cluster.
* [Thomas WOL: Working out Loud](https://github.com/guettli/wol) Articles, projects, and insights spanning various topics in software development.

## Feedback is welcome

Please create an issue if you have a question or a feature request.
