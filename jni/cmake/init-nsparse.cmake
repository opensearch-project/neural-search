#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

# The nsparse sources must already be checked out. Configure deliberately does NOT
# run `git submodule update --init` itself: pulling native code over the network as
# a side effect of `cmake -S jni` is invisible to the caller, and when it fails the
# error surfaces later as missing nsparse headers rather than as the real cause.
# CI checks the submodule out explicitly (submodules: recursive).
if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/external/neural-sparse-cpp/nsparse)
    message(FATAL_ERROR
            "neural-sparse-cpp submodule is not checked out at "
            "jni/external/neural-sparse-cpp.\n"
            "Run:  git submodule update --init --recursive")
endif ()

if (APPLE)
    message(STATUS "darwin macos detected for nsparse")
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
        message(STATUS "detected Mac with ARM architecture for nsparse")
        if(CMAKE_C_COMPILER_ID MATCHES "Clang\$")
            set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp")
            set(OpenMP_C_LIB_NAMES "omp")
            set(OpenMP_omp_LIBRARY /opt/homebrew/opt/libomp/lib/libomp.dylib)
        endif()

        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang\$")
            set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include")
            set(OpenMP_CXX_LIB_NAMES "omp")
            set(OpenMP_omp_LIBRARY /opt/homebrew/opt/libomp/lib/libomp.dylib)
        endif()
    else()
        message(STATUS "detected Mac with x86 architecture for nsparse")
        if(CMAKE_C_COMPILER_ID MATCHES "Clang\$")
            set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp")
            set(OpenMP_C_LIB_NAMES "omp")
            set(OpenMP_omp_LIBRARY /usr/local/opt/libomp/lib/libomp.dylib)
        endif()

        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang\$")
            set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I/usr/local/opt/libomp/include")
            set(OpenMP_CXX_LIB_NAMES "omp")
            set(OpenMP_omp_LIBRARY /usr/local/opt/libomp/lib/libomp.dylib)
        endif()
    endif()
endif()

# Set relevant properties
set(NSPARSE_ENABLE_PYTHON OFF)
set(NSPARSE_ENABLE_TESTS OFF)
set(NSPARSE_ENABLE_BENCHMARKS OFF)

# AVX2_ENABLED / AVX512_ENABLED / SVE_ENABLED select which nsparse variant to
# build. Exactly one variant is built per configure pass, and an instruction set the
# running CPU lacks is not a slow path -- it is SIGILL on the first vectorized call.
#
# They therefore default to what THIS host can actually execute, not to true.
# Defaulting them on assumed the build machine and the machine running the result
# were the same and both had AVX-512; a build host without it produced a library
# that died the moment it was called, which is what broke the Linux CI runners
# while Windows (pinned to the generic build) passed.
#
# Pass them explicitly to override, which is what a distribution build wants:
# scripts/build.sh forces each variant in turn to produce the full set, and
# NativeCpuFeatures then chooses between them from the target host's CPU flags.
#
# k-NN reached the same conclusion for its newest tier: init-faiss.cmake leaves
# AVX2_ENABLED/AVX512_ENABLED defaulting to true but probes the host with `lscpu`
# for AVX512_SPR_ENABLED. A compile-and-run check is used here instead of lscpu
# because lscpu is Linux-only, and the same argument applies to all three tiers.
function(detect_cpu_feature feature_name builtin_name out_var)
    if(DEFINED ${out_var})
        return()
    endif()
    if(CMAKE_CROSSCOMPILING)
        # Cannot run a probe on the build host and learn anything about the
        # target, so stay conservative.
        set(${out_var} false PARENT_SCOPE)
        message(STATUS "Cross-compiling: assuming no ${feature_name}")
        return()
    endif()
    include(CheckCXXSourceRuns)
    set(probe "int main() { return __builtin_cpu_supports(\"${builtin_name}\") ? 0 : 1; }")
    check_cxx_source_runs("${probe}" HAS_${feature_name})
    if(HAS_${feature_name})
        set(${out_var} true PARENT_SCOPE)
    else()
        set(${out_var} false PARENT_SCOPE)
    endif()
    message(STATUS "Build host supports ${feature_name}: ${HAS_${feature_name}}")
endfunction()

# MSVC has no __builtin_cpu_supports, and Windows is pinned to the generic build
# below regardless, so skip the probe rather than log a failed compile check.
if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86_64|AMD64" AND NOT MSVC)
    detect_cpu_feature(AVX2 "avx2" AVX2_ENABLED)
    detect_cpu_feature(AVX512 "avx512f" AVX512_ENABLED)
else()
    if(NOT DEFINED AVX2_ENABLED)
        set(AVX2_ENABLED false)
    endif()
    if(NOT DEFINED AVX512_ENABLED)
        set(AVX512_ENABLED false)
    endif()
endif()

# __builtin_cpu_supports has no SVE query, so probe by running SVE code instead --
# same reasoning as the tiers above, and a Graviton2 answers it with SIGILL, which
# check_cxx_source_runs reports as a failure. Only non-Apple aarch64 has an SVE
# branch in nsparse at all.
if(NOT DEFINED SVE_ENABLED)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64" AND NOT APPLE AND NOT CMAKE_CROSSCOMPILING AND NOT MSVC)
        include(CheckCXXSourceRuns)
        set(CMAKE_REQUIRED_FLAGS "-march=armv8-a+sve")
        check_cxx_source_runs("#include <arm_sve.h>\nint main() { return svcntb() > 0 ? 0 : 1; }" HAS_SVE)
        unset(CMAKE_REQUIRED_FLAGS)
        if(HAS_SVE)
            set(SVE_ENABLED true)
        else()
            set(SVE_ENABLED false)
        endif()
        message(STATUS "Build host supports SVE: ${HAS_SVE}")
    else()
        set(SVE_ENABLED false)
    endif()
endif()

# Determine optimization level and target library.
#
# The branches are ordered by architecture rather than by flag, because the flags
# are not mutually exclusive: SVE_ENABLED means nothing on x86_64 and the AVX
# toggles mean nothing on aarch64, so a flat chain of conditions ends up letting an
# irrelevant flag decide the variant.
#
# Windows is pinned to the generic build, matching k-NN ("SIMD optimization is not
# supported on Windows" in the OpenSearch docs) -- but NOT for the same reason, so
# do not treat this as a toolchain limitation. k-NN builds Windows with MinGW;
# this build uses MSVC, and nsparse carries MSVC /arch:AVX2 and /arch:AVX512
# branches. Building nsparse_avx512 here with MSVC was verified to work: it
# compiles clean, the test suite passes, and the DLL really does contain
# zmm-register instructions.
#
# The blocker is selection: NativeCpuFeatures reads /proc/cpuinfo to decide which
# variant is safe to load, and there is no equivalent on Windows, so an AVX-512 DLL
# there could only be picked by filename -- and dies on an illegal instruction on a
# host without AVX-512. Same on macOS. Both stay generic-only until the loader can
# verify the CPU on those platforms.
if(${CMAKE_SYSTEM_NAME} STREQUAL Windows)
    set(NSPARSE_OPT_LEVEL generic)
    set(TARGET_LINK_NSPARSE_LIB nsparse)
elseif(${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64|arm64")
    # Apple Silicon does not support SVE.
    if(SVE_ENABLED AND NOT APPLE)
        set(NSPARSE_OPT_LEVEL sve)
        set(TARGET_LINK_NSPARSE_LIB nsparse_sve)
        string(PREPEND LIB_EXT "_sve")
    else()
        set(NSPARSE_OPT_LEVEL generic)
        set(TARGET_LINK_NSPARSE_LIB nsparse)
    endif()
elseif(${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86_64|AMD64")
    # nsparse only has an avx512 target on Linux.
    if(AVX512_ENABLED AND ${CMAKE_SYSTEM_NAME} STREQUAL Linux)
        set(NSPARSE_OPT_LEVEL avx512)
        set(TARGET_LINK_NSPARSE_LIB nsparse_avx512)
        string(PREPEND LIB_EXT "_avx512")
    elseif(AVX2_ENABLED)
        set(NSPARSE_OPT_LEVEL avx2)
        set(TARGET_LINK_NSPARSE_LIB nsparse_avx2)
        string(PREPEND LIB_EXT "_avx2")
    else()
        set(NSPARSE_OPT_LEVEL generic)
        set(TARGET_LINK_NSPARSE_LIB nsparse)
    endif()
else()
    set(NSPARSE_OPT_LEVEL generic)
    set(TARGET_LINK_NSPARSE_LIB nsparse)
endif()
message(STATUS "nsparse optimization level: ${NSPARSE_OPT_LEVEL}")

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/neural-sparse-cpp EXCLUDE_FROM_ALL)
