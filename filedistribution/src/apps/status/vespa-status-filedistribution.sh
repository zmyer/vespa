#!/bin/sh
# Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

# BEGIN environment bootstrap section
# Do not edit between here and END as this section should stay identical in all scripts

findpath () {
    myname=${0}
    mypath=${myname%/*}
    myname=${myname##*/}
    if [ "$mypath" ] && [ -d "$mypath" ]; then
        return
    fi
    mypath=$(pwd)
    if [ -f "${mypath}/${myname}" ]; then
        return
    fi
    echo "FATAL: Could not figure out the path where $myname lives from $0"
    exit 1
}

COMMON_ENV=libexec/vespa/common-env.sh

source_common_env () {
    if [ "$VESPA_HOME" ] && [ -d "$VESPA_HOME" ]; then
        export VESPA_HOME
        common_env=$VESPA_HOME/$COMMON_ENV
        if [ -f "$common_env" ]; then
            . $common_env
            return
        fi
    fi
    return 1
}

findroot () {
    source_common_env && return
    if [ "$VESPA_HOME" ]; then
        echo "FATAL: bad VESPA_HOME value '$VESPA_HOME'"
        exit 1
    fi
    if [ "$ROOT" ] && [ -d "$ROOT" ]; then
        VESPA_HOME="$ROOT"
        source_common_env && return
    fi
    findpath
    while [ "$mypath" ]; do
        VESPA_HOME=${mypath}
        source_common_env && return
        mypath=${mypath%/*}
    done
    echo "FATAL: missing VESPA_HOME environment variable"
    echo "Could not locate $COMMON_ENV anywhere"
    exit 1
}

findroot

# END environment bootstrap section

ROOT=${VESPA_HOME%/}

if [ "$cloudconfig_server__disable_filedistributor" = "" ] || [ "$cloudconfig_server__disable_filedistributor" != "true" ]; then
    ZKSTRING=$($ROOT/libexec/vespa/vespa-config.pl -zkstring)
    test -z "$VESPA_LOG_LEVEL" && VESPA_LOG_LEVEL=warning
    export VESPA_LOG_LEVEL
    exec $ROOT/bin/vespa-status-filedistribution-bin --zkstring "$ZKSTRING" $@
else
    if [ "$cloudconfig_server__environment" != "" ]; then
        environment="--environment $cloudconfig_server__environment"
    fi
    if [ "$cloudconfig_server__region" != "" ]; then
        region="--region $cloudconfig_server__region"
    fi

    defaults="--tenant default --application default --instance default"
    jvmoptions="-XX:MaxJavaStackTraceDepth=-1 $(getJavaOptionsIPV46) -Xms48m -Xmx48m"
    jar="-cp $VESPA_HOME/lib/jars/filedistribution-jar-with-dependencies.jar"

    exec java $jvmoptions $jar com.yahoo.vespa.filedistribution.status.FileDistributionStatusClient $defaults $environment $region "$@"
fi
