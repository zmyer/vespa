#!/bin/sh
# Copyright 2016 Yahoo Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

if [ -z "$1" ]; then
  echo "Usage: $0 VERSION" 2>&1
  exit 1
fi

VERSION="$1"

mkdir -p ~/rpmbuild/{SOURCES,SPECS}
GZIP=-1 tar -zcf ~/rpmbuild/SOURCES/vespa-$VERSION.tar.gz --transform "flags=r;s,^,vespa-$VERSION/," *
sed -e "s,^Version: .*,Version: $VERSION,"  < vespa.spec > ~/rpmbuild/SPECS/vespa-$VERSION.spec
