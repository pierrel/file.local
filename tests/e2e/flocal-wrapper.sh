#!/bin/bash
# Tee stderr to a file the failure dump collects. The responder's stderr
# otherwise exists only on the connector's SSH channel, which the connector
# surfaces solely on its clean-exit path; stdout stays untouched because it
# is the protocol stream, and exec preserves the exit status.
exec 2> >(tee -a "${HOME:-/home/peer}/.flocal-stderr.log" >&2)
exec "${FLOCAL_E2E_EXECUTABLE:-/usr/local/libexec/flocal-real}" "$@"
