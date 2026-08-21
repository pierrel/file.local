#!/bin/sh
set -eu

probe="$(mktemp "${TMPDIR:-/tmp}/flocal-prefix.XXXXXX")"
rm -- "$probe"
trap 'rm -f -- "$probe"' EXIT HUP INT TERM
export PROBE="$probe"

prefix='relative $(shell touch "$$PROBE")'
actual="$(make -s test-make-prefix-value "PREFIX=$prefix")"
test "$actual" = "$prefix"
test ! -e "$probe"
