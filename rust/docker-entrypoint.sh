#!/bin/bash
cd bwk-dart || exit 1
bash linux.sh "$VERSION"
exec "$@"