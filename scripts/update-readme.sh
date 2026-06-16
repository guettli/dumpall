#!/usr/bin/env bash
# Bash Strict Mode: https://github.com/guettli/bash-strict-mode
trap 'echo -e "\nWarning: a command failed at ${BASH_SOURCE[0]}:$LINENO" >&2; exit 3' ERR
set -Eeuo pipefail

cd "$(dirname "$0")/.."

help_file="$(mktemp)"
output_file="$(mktemp)"
cleanup() {
    rm -f "$help_file" "$output_file"
}
trap cleanup EXIT

go run . --help >"$help_file"

awk -v help_file="$help_file" '
BEGIN {
    in_block = 0
}
/<!-- usage:start -->/ {
    print
    print "```text"
    while ((getline line < help_file) > 0) {
        print line
    }
    close(help_file)
    print "```"
    in_block = 1
    next
}
/<!-- usage:end -->/ {
    in_block = 0
    print
    next
}
!in_block {
    print
}
' README.md >"$output_file"

mv "$output_file" README.md
