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
