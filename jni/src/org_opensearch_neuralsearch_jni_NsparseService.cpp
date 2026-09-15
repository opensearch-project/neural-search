/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#include <jni.h>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>

#include "nsparse_wrapper.h"
#include "common.h"
#include "jni_util.h"
#include "nsparse/io/index_io.h"

namespace {

/**
 * Convert a Java Map<String, Object> to a C++ std::map<std::string, jobject>.
 * Uses cached class/method IDs for performance.
 */
std::map<std::string, jobject> javaMapToStdMap(JNIEnv* env, jobject jmap) {
    std::map<std::string, jobject> result;
    if (jmap == nullptr) return result;

    auto& c = neural_search_jni::cachedRefs;
    using neural_search_jni::ScopedLocalRef;
    using neural_search_jni::ScopedStringChars;

    ScopedLocalRef entrySet(
        env, env->CallObjectMethod(jmap, c.getMethod(neural_search_jni::MAP_ENTRY_SET)));
    ScopedLocalRef iterator(
        env, env->CallObjectMethod(entrySet.get(), c.getMethod(neural_search_jni::SET_ITERATOR)));

    while (env->CallBooleanMethod(iterator.get(), c.getMethod(neural_search_jni::ITERATOR_HAS_NEXT))) {
        // Every ref below is scoped: the local ref table holds 512 entries by
        // default, so leaking one per map entry per query eventually overflows it.
        ScopedLocalRef entry(
            env, env->CallObjectMethod(iterator.get(), c.getMethod(neural_search_jni::ITERATOR_NEXT)));
        ScopedLocalRef jkey(
            env, env->CallObjectMethod(entry.get(), c.getMethod(neural_search_jni::ENTRY_GET_KEY)));
        // The value ref is intentionally NOT scoped: it is stored in the returned
        // map and read by the caller, and stays valid for the duration of the
        // enclosing JNI call's local frame.
        jobject jvalue = env->CallObjectMethod(entry.get(), c.getMethod(neural_search_jni::ENTRY_GET_VALUE));

        result[ScopedStringChars(env, static_cast<jstring>(jkey.get())).toString()] = jvalue;
    }

    return result;
}

/**
 * Create a Java SparseQueryResult[] from C++ distance/label arrays.
 * Uses cached class ref and constructor ID.
 */
jobjectArray buildSparseQueryResults(JNIEnv* env, const float* distances,
                                     const int32_t* labels, int k) {
    auto& c = neural_search_jni::cachedRefs;
    jclass resultClass = c.getClass(neural_search_jni::SPARSE_QUERY_RESULT);
    jmethodID ctor = c.getMethod(neural_search_jni::SPARSE_QUERY_RESULT_CTOR);

    // nsparse pads the tail of a short result set with INVALID_IDX (-1); those
    // slots are not hits and must not reach Java, where -1 would read as a doc id.
    int validCount = 0;
    for (int i = 0; i < k; i++) {
        if (labels[i] != -1) validCount++;
    }

    jobjectArray results = env->NewObjectArray(validCount, resultClass, nullptr);
    if (results == nullptr) {
        env->ExceptionClear();
        throw std::bad_alloc();
    }
    int idx = 0;
    for (int i = 0; i < k; i++) {
        if (labels[i] == -1) continue;
        neural_search_jni::ScopedLocalRef obj(
            env, env->NewObject(resultClass, ctor, static_cast<jint>(labels[i]),
                                static_cast<jfloat>(distances[i])));
        if (obj.get() == nullptr) {
            env->ExceptionClear();
            throw std::bad_alloc();
        }
        env->SetObjectArrayElement(results, idx++, obj.get());
    }
    return results;
}

}  // anonymous namespace

// ============================================================================
// JNI_OnLoad — cache class refs and method IDs once at library load time.
// ============================================================================

extern "C" JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
    JNIEnv* env = nullptr;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        return JNI_ERR;
    }
    try {
        neural_search_jni::cachedRefs.init(env);
    } catch (const std::exception& e) {
        neural_search_jni::ThrowJavaException(env, "java/lang/RuntimeException", e.what());
        return JNI_ERR;
    }
    return JNI_VERSION_1_8;
}

extern "C" JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* reserved) {
    JNIEnv* env = nullptr;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) == JNI_OK) {
        neural_search_jni::cachedRefs.release(env);
    }
}

// ============================================================================
// JNI exported functions for org.opensearch.neuralsearch.jni.NativeLibrary
// ============================================================================

extern "C" {

JNIEXPORT jlong JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_initIndex(
    JNIEnv* env, jclass cls, jlong numDocs, jint dim, jobject parameters) {
    try {
        auto params = javaMapToStdMap(env, parameters);
        return neural_search_jni::nsparse_wrapper::initIndex(numDocs, dim, params, env);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
        return 0;
    }
}

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_insertToIndex(
    JNIEnv* env, jclass cls, jlong indexAddress, jintArray ids,
    jlong indicesAddress, jlong tokensAddress, jlong valueAddress,
    jint threadCount) {
    try {
        neural_search_jni::ScopedIntArray idElements(env, ids);

        neural_search_jni::nsparse_wrapper::insertToIndex(
            indexAddress,
            reinterpret_cast<const int32_t*>(idElements.data()),
            idElements.length(),
            indicesAddress, tokensAddress, valueAddress,
            threadCount);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_readCsrAndIdsToIndex(
    JNIEnv* env, jclass cls, jlong indexAddress, jstring csrPath,
    jstring idPath, jint threadCount) {
    try {
        std::string csr = neural_search_jni::ScopedStringChars(env, csrPath).toString();
        std::string ids = neural_search_jni::ScopedStringChars(env, idPath).toString();
        neural_search_jni::nsparse_wrapper::readCsrAndIdsToIndex(
            indexAddress, csr, ids, threadCount);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_writeIndex(
    JNIEnv* env, jclass cls, jlong indexAddress, jobject output) {
    try {
        neural_search_jni::nsparse_wrapper::writeIndex(indexAddress, output, env);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

JNIEXPORT jlong JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_loadIndex(
    JNIEnv* env, jclass cls, jstring indexPath) {
    try {
        std::string path = neural_search_jni::ScopedStringChars(env, indexPath).toString();
        // Ask for mmap residency so the posting lists and forward index are
        // borrowed from the file and accounted as reclaimable page cache. nsparse
        // ignores the flag for index types it cannot map, so segments written in
        // an older non-mappable format still load by copying.
        return neural_search_jni::nsparse_wrapper::loadIndex(
            path, nsparse::IndexIoFlag::kUseMmap);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
        return 0;
    }
}

JNIEXPORT jobjectArray JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_queryIndex(
    JNIEnv* env, jclass cls, jlong indexPointer, jintArray jtokens,
    jfloatArray jweights, jint k, jobject methodParameters) {
    try {
        neural_search_jni::ScopedIntArray tokenElements(env, jtokens);
        neural_search_jni::ScopedFloatArray weightElements(env, jweights);

        // -1 is the "no hit" sentinel buildSparseQueryResults() skips. Seed the
        // whole array with it: a search that fills fewer than k slots would
        // otherwise leave zero-initialized entries that read back as doc 0.
        std::vector<float> distances(k, 0.0f);
        std::vector<int32_t> labels(k, -1);

        auto params = javaMapToStdMap(env, methodParameters);
        neural_search_jni::nsparse_wrapper::queryIndex(
            indexPointer,
            reinterpret_cast<const int32_t*>(tokenElements.data()),
            weightElements.data(),
            tokenElements.length(), k, params, env,
            distances.data(), labels.data());

        return buildSparseQueryResults(env, distances.data(), labels.data(), k);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
        return nullptr;
    }
}

JNIEXPORT jobjectArray JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_queryIndexWithFilter(
    JNIEnv* env, jclass cls, jlong indexPointer, jintArray jtokens,
    jfloatArray jweights, jint k, jobject methodParameters,
    jlongArray jfilterIds, jint filterIdsType) {
    try {
        neural_search_jni::ScopedIntArray tokenElements(env, jtokens);
        neural_search_jni::ScopedFloatArray weightElements(env, jweights);
        neural_search_jni::ScopedLongArray filterIdElements(env, jfilterIds);

        std::vector<float> distances(k, 0.0f);
        std::vector<int32_t> labels(k, -1);

        auto params = javaMapToStdMap(env, methodParameters);
        // On some platforms jlong (long) != int64_t (long long), so copy
        std::vector<int64_t> filterIds64(
            filterIdElements.data(), filterIdElements.data() + filterIdElements.length());
        neural_search_jni::nsparse_wrapper::queryIndexWithFilter(
            indexPointer,
            reinterpret_cast<const int32_t*>(tokenElements.data()),
            weightElements.data(),
            tokenElements.length(), k, params,
            filterIds64.data(), filterIdElements.length(), filterIdsType,
            env, distances.data(), labels.data());

        return buildSparseQueryResults(env, distances.data(), labels.data(), k);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
        return nullptr;
    }
}

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_freeIndex(
    JNIEnv* env, jclass cls, jlong indexAddress) {
    try {
        neural_search_jni::nsparse_wrapper::freeIndex(indexAddress);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_transferVectors(
    JNIEnv* env, jclass cls, jlongArray jmemoryAddresses,
    jintArray jindices, jintArray jtokens, jfloatArray jweights) {
    try {
        // Release mode 0: transferVectors writes freshly allocated buffer
        // addresses back into memoryAddresses, and Java owns them from then on.
        // Scoped so the copy-back happens even if an allocation midway through
        // throws — otherwise the buffers already created would be unreachable.
        neural_search_jni::ScopedLongArray memAddrs(env, jmemoryAddresses, 0);
        neural_search_jni::ScopedIntArray indices(env, jindices);
        neural_search_jni::ScopedIntArray tokens(env, jtokens);
        neural_search_jni::ScopedFloatArray weights(env, jweights);

        neural_search_jni::transferVectors(
            reinterpret_cast<int64_t*>(memAddrs.data()),
            reinterpret_cast<const int32_t*>(indices.data()), indices.length(),
            reinterpret_cast<const int32_t*>(tokens.data()), tokens.length(),
            weights.data(), weights.length());
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_freeVectors(
    JNIEnv* env, jclass cls, jlongArray jmemoryAddresses) {
    try {
        // Release mode 0: freeVectors zeroes each address as it frees it, and Java
        // reads those zeroes back to know the buffer no longer owns anything.
        neural_search_jni::ScopedLongArray memAddrs(env, jmemoryAddresses, 0);
        neural_search_jni::freeVectors(
            reinterpret_cast<int64_t*>(memAddrs.data()));
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

}  // extern "C"
