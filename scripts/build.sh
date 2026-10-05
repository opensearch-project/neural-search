#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

# Builds the plugin zip with the native sparse engine (jni/) bundled in, for
# opensearch-build. Without this script opensearch-build falls back to its default
# plugin build, which only runs `./gradlew assemble` and so ships a zip with no
# native library in it -- the plugin then fails to load the moment a sparse index
# is used.

set -ex

function usage() {
    echo "Usage: $0 [args]"
    echo ""
    echo "Arguments:"
    echo -e "-v VERSION\t[Required] OpenSearch version."
    echo -e "-q QUALIFIER\t[Optional] Version qualifier."
    echo -e "-s SNAPSHOT\t[Optional] Build a snapshot, default is 'false'."
    echo -e "-p PLATFORM\t[Optional] Platform, default is the current platform."
    echo -e "-a ARCHITECTURE\t[Optional] Build architecture, default is the current architecture."
    echo -e "-o OUTPUT\t[Optional] Output path, default is 'artifacts'."
    echo -e "-j NPROC_COUNT\t[Optional] Number of CPUs to use when building the JNI library. Default is all of them."
    echo -e "-h help"
}

while getopts ":hv:q:s:o:p:a:j:" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
        v)
            VERSION=$OPTARG
            ;;
        q)
            QUALIFIER=$OPTARG
            ;;
        s)
            SNAPSHOT=$OPTARG
            ;;
        o)
            OUTPUT=$OPTARG
            ;;
        p)
            PLATFORM=$OPTARG
            ;;
        a)
            ARCHITECTURE=$OPTARG
            ;;
        j)
            NPROC_COUNT=$OPTARG
            ;;
        :)
            echo "Error: -${OPTARG} requires an argument"
            usage
            exit 1
            ;;
        ?)
            echo "Invalid option: -${arg}"
            exit 1
            ;;
    esac
done

if [ -z "$VERSION" ]; then
    echo "Error: You must specify the OpenSearch version"
    usage
    exit 1
fi

[[ ! -z "$QUALIFIER" ]] && VERSION=$VERSION-$QUALIFIER
[[ "$SNAPSHOT" == "true" ]] && VERSION=$VERSION-SNAPSHOT
[ -z "$OUTPUT" ] && OUTPUT=artifacts

# opensearch-build always passes -p/-a, but the script is also run by hand.
if [ -z "$PLATFORM" ]; then
    case "$(uname -s)" in
        Linux) PLATFORM=linux ;;
        Darwin) PLATFORM=darwin ;;
        CYGWIN*|MINGW*|MSYS*) PLATFORM=windows ;;
        *) echo "Error: unsupported platform $(uname -s)"; exit 1 ;;
    esac
fi
if [ -z "$ARCHITECTURE" ]; then
    case "$(uname -m)" in
        x86_64|amd64) ARCHITECTURE=x64 ;;
        aarch64|arm64) ARCHITECTURE=arm64 ;;
        *) echo "Error: unsupported architecture $(uname -m)"; exit 1 ;;
    esac
fi

work_dir=$PWD

# The native engine lives in a submodule. Check it out here rather than relying on
# cmake to do it: jni/cmake/init-nsparse.cmake deliberately refuses to fetch code
# over the network as a side effect of configure, and fails with a clear error
# instead of a wall of missing headers.
git submodule update --init --recursive -- jni/external/neural-sparse-cpp

# cmake resolves jni.h from $JAVA_HOME (see jni/CMakeLists.txt), so an unset
# JAVA_HOME fails late, as a missing header.
if [ -z "$JAVA_HOME" ]; then
    if [ "$PLATFORM" = "darwin" ]; then
        export JAVA_HOME=`/usr/libexec/java_home`
        echo "SET JAVA_HOME=$JAVA_HOME"
    else
        echo "Error: JAVA_HOME is not set and is required to build the JNI library"
        exit 1
    fi
fi

# Build the plugin zip and the Java artifacts. The JNI variants are built
# separately below, so keep the native build out of this pass: `assemble` does not
# depend on buildJniLib, and the test tasks that do are excluded.
#
# -Pcrypto.standard carries over from the default build script this one replaces, so
# a FIPS distribution build keeps behaving the way it does today.
echo "Building neural-search plugin"
./gradlew assemble --no-daemon --refresh-dependencies -DskipTests=true \
    -Dopensearch.version=$VERSION -Dbuild.snapshot=$SNAPSHOT -Dbuild.version_qualifier=$QUALIFIER \
    -Pcrypto.standard=FIPS-140-3

# Publish before the native libraries are added to the zip, matching k-NN: the
# maven artifact stays a plain plugin zip (see DEVELOPER_GUIDE.md), while the zip
# copied to $OUTPUT/plugins -- the one the distribution installs -- gets the libs.
./gradlew publishPluginZipPublicationToMavenLocal -Dopensearch.version=$VERSION -Dbuild.snapshot=$SNAPSHOT -Dbuild.version_qualifier=$QUALIFIER -Pcrypto.standard=FIPS-140-3
./gradlew publishPluginZipPublicationToZipStagingRepository -Dopensearch.version=$VERSION -Dbuild.snapshot=$SNAPSHOT -Dbuild.version_qualifier=$QUALIFIER -Pcrypto.standard=FIPS-140-3

# Build one JNI library per SIMD variant this architecture can use.
#
# Only one variant is compiled per pass -- nsparse picks its target from these
# flags -- and the resulting file carries the variant in its name, so the passes
# accumulate in jni/build instead of overwriting each other. NativeCpuFeatures
# then picks between them from /proc/cpuinfo at load time; building a variant here
# that the loader cannot verify at runtime would just be a SIGILL waiting to
# happen, which is why macOS and Windows stay generic-only (init-nsparse.cmake
# pins them there as well).
#
# The cmake cache has to go between passes: the flags below feed nsparse's target
# selection through cached variables, and a stale cache silently rebuilds the
# variant from the previous pass.
function build_jni_variant() {
    local description=$1
    shift
    echo "Building the native sparse engine: ${description}"
    rm -rf jni/build/CMakeCache.txt jni/build/CMakeFiles
    # An unset -j leaves the parallelism at build.gradle's default, the core count.
    ./gradlew :buildJniLib "$@" ${NPROC_COUNT:+-Dnproc.count=$NPROC_COUNT} \
        -Dopensearch.version=$VERSION -Dbuild.snapshot=$SNAPSHOT
}

if [ "$PLATFORM" = "linux" ] && [ "$ARCHITECTURE" = "x64" ]; then
    build_jni_variant "generic" -Davx2.enabled=false -Davx512.enabled=false
    build_jni_variant "avx2" -Davx2.enabled=true -Davx512.enabled=false
    build_jni_variant "avx512" -Davx2.enabled=true -Davx512.enabled=true
elif [ "$PLATFORM" = "linux" ] && [ "$ARCHITECTURE" = "arm64" ]; then
    build_jni_variant "generic" -Dsve.enabled=false
    build_jni_variant "sve" -Dsve.enabled=true
else
    # macOS and Windows: NativeCpuFeatures cannot read CPU flags there, so a SIMD
    # variant could never be selected safely even if it were built.
    build_jni_variant "generic" -Davx2.enabled=false -Davx512.enabled=false -Dsve.enabled=false
fi

# Add the native libraries to the plugin zip. A top-level lib/ in the zip is
# extracted into plugins/opensearch-neural-search/lib -- only a top-level shared/ is
# relocated by opensearch-plugin, so this layout survives installation.
#
# Getting that directory onto the loader's search path is NOT part of the plugin:
# opensearch-build's release Dockerfiles set LD_LIBRARY_PATH to
# $OPENSEARCH_HOME/plugins/opensearch-knn/lib, and the JVM folds LD_LIBRARY_PATH into
# java.library.path on Linux. The same entry has to be added there for
# opensearch-neural-search, or System.loadLibrary will not find these files no matter
# how the zip is built.
#
# Name the zip rather than searching for it: build/distributions keeps the output of
# every build ever run in this workspace, so a glob there picks up the zip of some
# older version as readily as this one. The name is what build.gradle derives from
# -Dopensearch.version -- the OpenSearch version with a plugin patch number appended.
distributions=$work_dir/build/distributions
pluginVersion=${VERSION%%-*}.0
[[ ! -z "$QUALIFIER" ]] && pluginVersion=$pluginVersion-$QUALIFIER
[[ "$SNAPSHOT" == "true" ]] && pluginVersion=$pluginVersion-SNAPSHOT
zipPath=$distributions/opensearch-neural-search-${pluginVersion}.zip

if [ ! -f "$zipPath" ]; then
    echo "Error: expected the plugin zip at $zipPath. build/distributions holds:"
    ls -l $distributions
    exit 1
fi

# A lib/ left behind by an earlier run would put its libraries in this zip too.
rm -rf $distributions/lib
mkdir -p $distributions/lib

if [ "$PLATFORM" = "windows" ]; then
    cp -v ./jni/build/opensearch_neuralsearch_nsparse*.dll $distributions/lib
    # No OpenMP runtime is copied: this build uses MSVC, whose vcomp140.dll comes
    # from the Visual C++ redistributable and is not ours to redistribute.
else
    cp -v ./jni/build/libopensearch_neuralsearch_nsparse* $distributions/lib

    # nsparse itself is a static library, so the JNI library's only non-system
    # dependency is the OpenMP runtime. Ship the copy it was linked against;
    # resolve it from the library rather than from a hard-coded name so gcc's
    # libgomp and clang's libomp are both handled.
    #
    # Probe the generic variant by name: every variant comes out of the same
    # toolchain and so links the same runtime, the generic one is built on every
    # platform, and a glob here would hand ldd/otool several libraries at once.
    if [ "$PLATFORM" = "darwin" ]; then
        probeLib=$distributions/lib/libopensearch_neuralsearch_nsparse.jnilib
        ompPath=$(otool -L $probeLib | grep -o '\S*libomp[^ ]*\.dylib' | head -n 1)
    else
        probeLib=$distributions/lib/libopensearch_neuralsearch_nsparse.so
        ompPath=$(ldd $probeLib | grep -o '/\S*libgomp[^ ]*' | head -n 1)
    fi
    if [ -f "$ompPath" ]; then
        cp -v $ompPath $distributions/lib
    else
        echo "WARNING: could not resolve the OpenMP runtime; the node must provide it"
    fi
fi
ls -l $distributions/lib

cd $distributions
zip -ur $zipPath lib
cd $work_dir

echo "COPY ${zipPath}"
mkdir -p $OUTPUT/plugins
cp -v $zipPath $OUTPUT/plugins

mkdir -p $OUTPUT/maven/org/opensearch
cp -r ./build/local-staging-repo/org/opensearch/. $OUTPUT/maven/org/opensearch
