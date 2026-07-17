#!/bin/sh
# Container PID 1: sshd bounded by a lifetime so a container whose harness
# was killed still terminates (and, with --rm, removes itself) on its own.
exec /usr/bin/timeout "${FLOCAL_E2E_CONTAINER_LIFETIME_SECONDS:-3600}" \
    /usr/sbin/sshd -D -e
