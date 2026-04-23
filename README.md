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
rules:
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

Rules match by `group`, `kind`, `namespace`, and `name` using globbing. If one of these matchers is omitted, it behaves like `*`. Cluster-scoped resources use the namespace `_cluster` for matching. Fields use dot notation, literal dots in map keys must be escaped as `\.` and individual path segments can use `*` or `?` glob wildcards, for example `metadata.labels.kapp\.k14s\.io/*`. The token `...` means recursive descent across zero or more nested levels, and paths automatically walk lists, so `webhooks.clientConfig.caBundle` removes `caBundle` from every webhook entry.

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
