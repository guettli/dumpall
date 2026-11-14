# Dumpall Manual

Read resources from the api-server (or a YAML file/directory via --read-yaml-from) and dump each resource to a file.

The normalization is done by kubenorm (https://github.com/guettli/kubenorm).
For YAML files on disk (helm output, Rendered Manifests Pattern, CI checks)
use kubenorm directly.

```text
dumpall [command] [global flags] [command flags]
```

### Commands
- [dumpall check-normalized](#dumpall-check-normalized)
- [dumpall completion](#dumpall-completion)
- [dumpall completion bash](#dumpall-completion-bash)
- [dumpall completion fish](#dumpall-completion-fish)
- [dumpall completion help](#dumpall-completion-help)
- [dumpall completion powershell](#dumpall-completion-powershell)
- [dumpall completion zsh](#dumpall-completion-zsh)
- [dumpall diff](#dumpall-diff)
- [dumpall gendocs](#dumpall-gendocs)
- [dumpall help](#dumpall-help)
- [dumpall show-common-ignore-config](#dumpall-show-common-ignore-config)
- [dumpall top-churn](#dumpall-top-churn)
- [dumpall version](#dumpall-version)


# Commands

## `dumpall check-normalized`

Check whether a YAML file or directory is already normalized.
Exits 0 if already normalized, 1 if normalization would change it.

```text
dumpall check-normalized <yaml-file-or-dir> [flags]
```

### Command Flags

```text
  -s, --dump-secrets                Include secret values
      --exclude-namespaces string   Comma-separated list of namespace globs to exclude
  -h, --help                        help for check-normalized
      --ignore-config file          Path to a YAML file with additional ignore rules
      --kind string                 Comma-separated list of kind globs to filter (e.g. 'ConfigMap,Secret')
  -x, --name-regex string           Only check resources where metadata.name matches this regex
  -n, --namespaces string           Comma-separated list of namespaces to filter
      --no-common-ignore-config     Disable the embedded common ignore config
  -q, --quiet                       Suppress progress output (default true)
      --skip-name-glob string       Skip resources where metadata.name matches this glob
  -O, --skip-owned                  Skip resources with a controlling owner reference
```

## `dumpall completion`

Generate the autocompletion script for dumpall for the specified shell.
See each sub-command's help for details on how to use the generated script.


```text
dumpall completion [flags]
```

### Command Flags

```text
  -h, --help   help for completion
```

## `dumpall completion bash`

Generate the autocompletion script for the bash shell.

This script depends on the 'bash-completion' package.
If it is not installed already, you can install it via your OS's package manager.

To load completions in your current shell session:

	source <(dumpall completion bash)

To load completions for every new session, execute once:

#### Linux:

	dumpall completion bash > /etc/bash_completion.d/dumpall

#### macOS:

	dumpall completion bash > $(brew --prefix)/etc/bash_completion.d/dumpall

You will need to start a new shell for this setup to take effect.


```text
dumpall completion bash
```

### Command Flags

```text
  -h, --help              help for bash
      --no-descriptions   disable completion descriptions
```

## `dumpall completion fish`

Generate the autocompletion script for the fish shell.

To load completions in your current shell session:

	dumpall completion fish | source

To load completions for every new session, execute once:

	dumpall completion fish > ~/.config/fish/completions/dumpall.fish

You will need to start a new shell for this setup to take effect.


```text
dumpall completion fish [flags]
```

### Command Flags

```text
  -h, --help              help for fish
      --no-descriptions   disable completion descriptions
```

## `dumpall completion help`

Help provides help for any command in the application.
Simply type completion help [path to command] for full details.

```text
dumpall completion help [command] [flags]
```

### Command Flags

```text
  -h, --help   help for help
```

## `dumpall completion powershell`

Generate the autocompletion script for powershell.

To load completions in your current shell session:

	dumpall completion powershell | Out-String | Invoke-Expression

To load completions for every new session, add the output of the above command
to your powershell profile.


```text
dumpall completion powershell [flags]
```

### Command Flags

```text
  -h, --help              help for powershell
      --no-descriptions   disable completion descriptions
```

## `dumpall completion zsh`

Generate the autocompletion script for the zsh shell.

If shell completion is not already enabled in your environment you will need
to enable it.  You can execute the following once:

	echo "autoload -U compinit; compinit" >> ~/.zshrc

To load completions in your current shell session:

	source <(dumpall completion zsh)

To load completions for every new session, execute once:

#### Linux:

	dumpall completion zsh > "${fpath[1]}/_dumpall"

#### macOS:

	dumpall completion zsh > $(brew --prefix)/share/zsh/site-functions/_dumpall

You will need to start a new shell for this setup to take effect.


```text
dumpall completion zsh [flags]
```

### Command Flags

```text
  -h, --help              help for zsh
      --no-descriptions   disable completion descriptions
```

## `dumpall diff`

Diff the current cluster state against a local dump directory.
Both sides are normalized before comparing (common ignore config applied by default).

```text
dumpall diff <local-dump-dir> [flags]
```

### Command Flags

```text
  -s, --dump-secrets                Include secret values in comparison
      --exclude-namespaces string   Comma-separated list of namespace globs to exclude
  -h, --help                        help for diff
      --ignore-config file          Path to a YAML file with ignore rules
      --kind string                 Comma-separated list of kind globs to compare (e.g. 'ConfigMap,Secret')
  -x, --name-regex string           Only compare resources where metadata.name matches this regex
  -n, --namespaces string           Comma-separated list of namespaces to compare
      --no-common-ignore-config     Disable the embedded common ignore config (compare raw resources)
  -q, --quiet                       Suppress progress output (default true)
      --read-yaml-from string       Read YAML from file/dir (source A) instead of connecting to cluster
      --skip-name-glob string       Skip resources where metadata.name matches this glob
  -O, --skip-owned                  Skip resources with a controlling owner reference
```

## `dumpall gendocs`

Generate usage.md from the command tree

```text
dumpall gendocs [flags]
```

### Command Flags

```text
  -h, --help   help for gendocs
```

## `dumpall help`

Help provides help for any command in the application.
Simply type dumpall help [path to command] for full details.

```text
dumpall help [command] [flags]
```

### Command Flags

```text
  -h, --help   help for help
```

## `dumpall show-common-ignore-config`

Print the embedded common ignore config

```text
dumpall show-common-ignore-config [flags]
```

### Command Flags

```text
  -h, --help   help for show-common-ignore-config
```

## `dumpall top-churn`

Compare two dump directories and list resources sorted by generation increase per hour.
Both dumps must have been created from the same cluster.

With --dump-wait-dump the command takes both dumps itself, waits between them, then compares.
The dump directories are kept on disk unless --remove is given.
Use --namespaces and --kind to limit the scope of the automatic dumps (both accept comma-separated values).

```text
dumpall top-churn [<dump-a> <dump-b>] [flags]
```

### Command Flags

```text
      --dump-wait-dump string[="7m"]   Take two dumps with a wait between them, then compare (default wait: 7m)
  -h, --help                           help for top-churn
      --kind string                    Comma-separated list of kind globs to dump (for --dump-wait-dump, e.g. 'Deployment,StatefulSet')
  -n, --namespaces string              Comma-separated list of namespaces to dump (for --dump-wait-dump)
      --remove                         Remove the dump directories after comparison (for --dump-wait-dump)
```

## `dumpall version`

Print the version

```text
dumpall version [flags]
```

### Command Flags

```text
  -h, --help   help for version
```
