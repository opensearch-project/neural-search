/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OPENSEARCH_NEURALSEARCH_JNI_UTIL_H__
#define __OPENSEARCH_NEURALSEARCH_JNI_UTIL_H__

#include <jni.h>
#include <string>
#include <unordered_map>
#include <utility>

namespace neural_search_jni {

// ============================================================================
// Cached class key constants
// ============================================================================
inline const char* JAVA_MAP              = "java/util/Map";
inline const char* JAVA_SET              = "java/util/Set";
inline const char* JAVA_ITERATOR         = "java/util/Iterator";
inline const char* JAVA_MAP_ENTRY        = "java/util/Map$Entry";
inline const char* JAVA_NUMBER           = "java/lang/Number";
inline const char* JAVA_BOOLEAN          = "java/lang/Boolean";
inline const char* SPARSE_QUERY_RESULT   = "org/opensearch/neuralsearch/sparse/common/SparseQueryResult";
inline const char* JAVA_OOM_ERROR        = "java/lang/OutOfMemoryError";
inline const char* JAVA_ILLEGAL_ARGUMENT = "java/lang/IllegalArgumentException";
inline const char* JAVA_EXCEPTION        = "java/lang/Exception";
inline const char* JAVA_RUNTIME_EXCEPTION = "java/lang/RuntimeException";
inline const char* JAVA_THROWABLE        = "java/lang/Throwable";

// ============================================================================
// Cached method key constants
// ============================================================================
inline const char* MAP_ENTRY_SET         = "MAP_ENTRY_SET";
inline const char* SET_ITERATOR          = "SET_ITERATOR";
inline const char* ITERATOR_HAS_NEXT     = "ITERATOR_HAS_NEXT";
inline const char* ITERATOR_NEXT         = "ITERATOR_NEXT";
inline const char* ENTRY_GET_KEY         = "ENTRY_GET_KEY";
inline const char* ENTRY_GET_VALUE       = "ENTRY_GET_VALUE";
inline const char* NUMBER_FLOAT_VALUE    = "NUMBER_FLOAT_VALUE";
inline const char* NUMBER_INT_VALUE      = "NUMBER_INT_VALUE";
inline const char* BOOLEAN_VALUE         = "BOOLEAN_VALUE";
inline const char* SPARSE_QUERY_RESULT_CTOR = "SPARSE_QUERY_RESULT_CTOR";
inline const char* THROWABLE_TO_STRING   = "THROWABLE_TO_STRING";

// ============================================================================
// JniCachedRefs — map-based cache for class refs and method IDs
// ============================================================================

struct JniCachedRefs {
    std::unordered_map<std::string, jclass>    cachedClasses;
    std::unordered_map<std::string, jmethodID> cachedMethods;

    jclass getClass(const char* key) const;
    jmethodID getMethod(const char* key) const;
    void init(JNIEnv* env);
    void release(JNIEnv* env);

private:
    void cacheClass(JNIEnv* env, const char* className);
    void cacheMethod(JNIEnv* env, const char* methodKey,
                     const char* classKey, const char* methodName,
                     const char* sig);
};

// ============================================================================
// RAII guards
//
// The JNI entry points call into C++ that can throw. Hand-written
// Get.../Release... pairs skip the release whenever anything between them
// throws, which strands the pinned (or copied) buffer for the life of the JVM
// and, for a copy-back release, silently discards values the native side wrote.
// These guards release on every exit path.
// ============================================================================

namespace detail {
inline jint* getArrayElements(JNIEnv* env, jintArray a) { return env->GetIntArrayElements(a, nullptr); }
inline jfloat* getArrayElements(JNIEnv* env, jfloatArray a) { return env->GetFloatArrayElements(a, nullptr); }
inline jlong* getArrayElements(JNIEnv* env, jlongArray a) { return env->GetLongArrayElements(a, nullptr); }

inline void releaseArrayElements(JNIEnv* env, jintArray a, jint* p, jint mode) {
    env->ReleaseIntArrayElements(a, p, mode);
}
inline void releaseArrayElements(JNIEnv* env, jfloatArray a, jfloat* p, jint mode) {
    env->ReleaseFloatArrayElements(a, p, mode);
}
inline void releaseArrayElements(JNIEnv* env, jlongArray a, jlong* p, jint mode) {
    env->ReleaseLongArrayElements(a, p, mode);
}
}  // namespace detail

/**
 * Scoped access to a Java primitive array's elements.
 *
 * A null array yields data() == nullptr and length() == 0 rather than faulting,
 * so callers can accept optional arrays without a separate branch.
 *
 * releaseMode is the JNI release mode: JNI_ABORT for read-only access (the
 * default, no copy-back), or 0 when the native side mutates the buffer and the
 * changes must reach Java.
 */
template <typename JArrayT, typename ElemT>
class ScopedArrayElements {
public:
    ScopedArrayElements(JNIEnv* env, JArrayT array, jint releaseMode = JNI_ABORT)
        : env_(env),
          array_(array),
          releaseMode_(releaseMode),
          data_(array == nullptr ? nullptr : detail::getArrayElements(env, array)),
          length_(array == nullptr ? 0 : env->GetArrayLength(array)) {}

    ~ScopedArrayElements() {
        if (data_ != nullptr) {
            detail::releaseArrayElements(env_, array_, data_, releaseMode_);
        }
    }

    ScopedArrayElements(const ScopedArrayElements&) = delete;
    ScopedArrayElements& operator=(const ScopedArrayElements&) = delete;

    ElemT* data() const { return data_; }
    jsize length() const { return length_; }

private:
    JNIEnv* env_;
    JArrayT array_;
    jint releaseMode_;
    ElemT* data_;
    jsize length_;
};

using ScopedIntArray = ScopedArrayElements<jintArray, jint>;
using ScopedFloatArray = ScopedArrayElements<jfloatArray, jfloat>;
using ScopedLongArray = ScopedArrayElements<jlongArray, jlong>;

/**
 * Scoped local reference. The local ref table is small (512 entries by default),
 * so refs created in a loop must be released as they go rather than left to the
 * frame pop.
 */
class ScopedLocalRef {
public:
    ScopedLocalRef(JNIEnv* env, jobject ref) : env_(env), ref_(ref) {}
    ~ScopedLocalRef() {
        if (ref_ != nullptr) env_->DeleteLocalRef(ref_);
    }

    ScopedLocalRef(const ScopedLocalRef&) = delete;
    ScopedLocalRef& operator=(const ScopedLocalRef&) = delete;
    ScopedLocalRef(ScopedLocalRef&& other) noexcept
        : env_(other.env_), ref_(std::exchange(other.ref_, nullptr)) {}

    jobject get() const { return ref_; }

private:
    JNIEnv* env_;
    jobject ref_;
};

/**
 * Scoped access to a Java String's UTF-8 chars.
 */
class ScopedStringChars {
public:
    ScopedStringChars(JNIEnv* env, jstring str)
        : env_(env), str_(str), chars_(str == nullptr ? nullptr : env->GetStringUTFChars(str, nullptr)) {}
    ~ScopedStringChars() {
        if (chars_ != nullptr) env_->ReleaseStringUTFChars(str_, chars_);
    }

    ScopedStringChars(const ScopedStringChars&) = delete;
    ScopedStringChars& operator=(const ScopedStringChars&) = delete;

    const char* get() const { return chars_; }
    std::string toString() const { return chars_ == nullptr ? std::string() : std::string(chars_); }

private:
    JNIEnv* env_;
    jstring str_;
    const char* chars_;
};

// ============================================================================
// Exception helpers
// ============================================================================

void ThrowJavaException(JNIEnv* env, const char* classKey, const char* message);
void CatchCppExceptionAndThrowJava(JNIEnv* env);

/**
 * If a Java exception is pending, describe it and clear it so the caller can
 * throw a C++ exception without JNI's "exception pending" rules biting, then
 * return the original exception's toString(). Returns an empty string when
 * nothing was pending.
 *
 * Callers use this to keep the ORIGINAL Java failure (say an IOException from
 * IndexOutput) in the message instead of replacing it with a generic one.
 */
std::string DescribeAndClearPendingException(JNIEnv* env);

// ============================================================================
// Global singleton (defined in jni_util.cpp)
// ============================================================================
extern JniCachedRefs cachedRefs;

}  // namespace neural_search_jni

#endif //__OPENSEARCH_NEURALSEARCH_JNI_UTIL_H__
