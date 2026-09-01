#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

cmake_minimum_required(VERSION 3.24.0)

project(NeuralSearch_JNI)

# ---------------------------------- SETUP ----------------------------------
set(TARGET_LIB_NSPARSE opensearch_neuralsearch_nsparse)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
option(CONFIG_TEST "Build tests (downloads googletest)" OFF)
option(ENABLE_SANITIZERS "Build with AddressSanitizer and LeakSanitizer" OFF)

# The JNI layer hands raw addresses across the language boundary and frees them on
# the native side, so leaks and use-after-free are the failure modes that test
# assertions cannot see. The sanitizer build is what actually covers them.
#
# UBSan is deliberately not enabled: abseil (pulled in by nsparse) fails to compile
# under -fsanitize=undefined on GCC, because the instrumentation defeats constexpr
# evaluation of the function-pointer comparisons in hash_policy_traits.h.
if (ENABLE_SANITIZERS)
    message(STATUS "Sanitizers enabled (address, leak)")
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address)
endif()

# Set OS specific variables
if (${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
    set(CMAKE_MACOSX_RPATH 1)
    set(JVM_OS_TYPE darwin)
    set(LIB_EXT .jnilib)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Linux)
    set(JVM_OS_TYPE linux)
    set(LIB_EXT .so)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Windows)
    set(JVM_OS_TYPE win32)
    set(LIB_EXT .dll)
    set(CMAKE_SHARED_LIBRARY_PREFIX "")
    set(CMAKE_STATIC_LIBRARY_PREFIX "")
    # -fpermissive is a GCC/MinGW flag. MSVC only answers it with
    # "warning D9002: ignoring unknown option", once per translation unit, which
    # buries real diagnostics under abseil's build output.
    if (NOT MSVC)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpermissive")
    endif()
else()
    message(FATAL_ERROR "Unable to run on system: ${CMAKE_SYSTEM_NAME}")
endif()

# Multi-config generators (Visual Studio) default to emitting into build/<Config>/,
# but java.library.path and the jniTest task both point at jni/build. Pin every
# configuration to the top of the build tree so the layout matches single-config
# generators on Linux and macOS.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
foreach(OUTPUT_CONFIG IN LISTS CMAKE_CONFIGURATION_TYPES)
    string(TOUPPER ${OUTPUT_CONFIG} OUTPUT_CONFIG_UPPER)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUT_CONFIG_UPPER} ${CMAKE_BINARY_DIR})
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${OUTPUT_CONFIG_UPPER} ${CMAKE_BINARY_DIR})
endforeach()

# ----------------------------------------------------------------------------

# ---------------------------------- NSPARSE ----------------------------------
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/init-nsparse.cmake)
find_package(OpenMP REQUIRED)

# The JNI sources compile once into an object library that both the shared library
# and the test binary consume.
#
# The tests must NOT link against the shared library: on Windows a DLL exports only
# what is marked __declspec(dllexport), which here is just the JNIEXPORT'd entry
# points, so every internal symbol the tests touch (cachedRefs, JniCachedRefs::init,
# the nsparse_wrapper functions, NsparseStreamWriter) came back as an unresolved
# external under MSVC. ELF exports everything by default, which is why linking
# against the .so happened to work on Linux.
set(TARGET_OBJ_NSPARSE nsparse_jni_objects)
add_library(
    ${TARGET_OBJ_NSPARSE} OBJECT
    ${CMAKE_CURRENT_SOURCE_DIR}/src/org_opensearch_neuralsearch_jni_NsparseService.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/nsparse_wrapper.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/nsparse_stream_writer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/jni_util.cpp
)
target_include_directories(${TARGET_OBJ_NSPARSE} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    $ENV{JAVA_HOME}/include
    $ENV{JAVA_HOME}/include/${JVM_OS_TYPE}
    ${CMAKE_CURRENT_SOURCE_DIR}/external/neural-sparse-cpp
)
target_link_libraries(${TARGET_OBJ_NSPARSE} PUBLIC ${TARGET_LINK_NSPARSE_LIB} OpenMP::OpenMP_CXX)
set_target_properties(${TARGET_OBJ_NSPARSE} PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(${TARGET_LIB_NSPARSE} SHARED $<TARGET_OBJECTS:${TARGET_OBJ_NSPARSE}>)
target_link_libraries(${TARGET_LIB_NSPARSE} PRIVATE ${TARGET_OBJ_NSPARSE})
set_target_properties(${TARGET_LIB_NSPARSE} PROPERTIES SUFFIX ${LIB_EXT})
set_target_properties(${TARGET_LIB_NSPARSE} PROPERTIES POSITION_INDEPENDENT_CODE ON)

# nsparse links statically, so the OpenMP runtime is the only dependency that has to
# travel with the plugin. scripts/build.sh puts it next to this library inside the
# zip, but System.loadLibrary's java.library.path does not apply to a library's own
# dependencies -- the dynamic loader resolves those, and without an rpath it would
# only ever find a system-wide copy. Look beside ourselves first so the shipped copy
# is the one that gets used.
if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL Windows)
    if (APPLE)
        set(NSPARSE_ORIGIN "@loader_path")
    else()
        set(NSPARSE_ORIGIN "$ORIGIN")
    endif()
    set_target_properties(${TARGET_LIB_NSPARSE} PROPERTIES BUILD_RPATH "${NSPARSE_ORIGIN}" INSTALL_RPATH "${NSPARSE_ORIGIN}")
endif()
# ---------------------------------------------------------------------------

# --------------------------------- TESTS -----------------------------------
if (CONFIG_TEST)
    # Reference - https://crascit.com/2015/07/25/cmake-gtest/
    configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
    execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
            WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googletest-download"
            )
    execute_process(COMMAND "${CMAKE_COMMAND}" --build .
            WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googletest-download"
            )
    # Required on Windows: the JNI library and nsparse use the shared CRT, and
    # googletest defaults to the static one, which cannot be mixed in one binary.
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

    add_subdirectory("${CMAKE_BINARY_DIR}/googletest-src"
            "${CMAKE_BINARY_DIR}/googletest-build" EXCLUDE_FROM_ALL
            )
    add_executable(
            jni_test
            tests/nsparse_wrapper_test.cpp
            tests/test_util.cpp
            tests/commons_test.cpp
            $<TARGET_OBJECTS:${TARGET_OBJ_NSPARSE}>
    )

    target_link_libraries(
            jni_test
            gtest_main
            gmock_main
            ${TARGET_OBJ_NSPARSE}
    )

    target_include_directories(jni_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${gtest_SOURCE_DIR}/include
            ${gmock_SOURCE_DIR}/include)

    if (ENABLE_SANITIZERS)
        # The ASan runtime lives beside the compiler rather than on the default
        # loader path on several distributions, so record its directory in the
        # test binary's RPATH instead of relying on ldconfig.
        execute_process(
                COMMAND ${CMAKE_CXX_COMPILER} -print-file-name=libasan.so
                OUTPUT_VARIABLE ASAN_RUNTIME_PATH
                OUTPUT_STRIP_TRAILING_WHITESPACE)
        get_filename_component(ASAN_RUNTIME_DIR "${ASAN_RUNTIME_PATH}" DIRECTORY)
        if (ASAN_RUNTIME_DIR)
            message(STATUS "ASan runtime directory: ${ASAN_RUNTIME_DIR}")
            set_target_properties(jni_test PROPERTIES BUILD_RPATH "${ASAN_RUNTIME_DIR}")
        endif()
    endif()
endif()
# ---------------------------------------------------------------------------
