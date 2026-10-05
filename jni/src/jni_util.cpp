/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "jni_util.h"

#include <new>
#include <stdexcept>

namespace neural_search_jni {

// ============================================================================
// Global singleton
// ============================================================================
JniCachedRefs cachedRefs;

// ============================================================================
// JniCachedRefs implementation
// ============================================================================

jclass JniCachedRefs::getClass(const char* key) const {
    auto it = cachedClasses.find(key);
    if (it == cachedClasses.end()) {
        throw std::runtime_error(std::string("Cached class not found: ") + key);
    }
    return it->second;
}

jmethodID JniCachedRefs::getMethod(const char* key) const {
    auto it = cachedMethods.find(key);
    if (it == cachedMethods.end()) {
        throw std::runtime_error(std::string("Cached method not found: ") + key);
    }
    return it->second;
}

void JniCachedRefs::init(JNIEnv* env) {
    // Cache classes
    cacheClass(env, JAVA_MAP);
    cacheClass(env, JAVA_SET);
    cacheClass(env, JAVA_ITERATOR);
    cacheClass(env, JAVA_MAP_ENTRY);
    cacheClass(env, JAVA_NUMBER);
    cacheClass(env, JAVA_BOOLEAN);
    cacheClass(env, SPARSE_QUERY_RESULT);
    cacheClass(env, JAVA_OOM_ERROR);
    cacheClass(env, JAVA_ILLEGAL_ARGUMENT);
    cacheClass(env, JAVA_EXCEPTION);
    cacheClass(env, JAVA_RUNTIME_EXCEPTION);
    cacheClass(env, JAVA_THROWABLE);

    // Cache method IDs
    cacheMethod(env, MAP_ENTRY_SET,
                JAVA_MAP, "entrySet", "()Ljava/util/Set;");
    cacheMethod(env, SET_ITERATOR,
                JAVA_SET, "iterator", "()Ljava/util/Iterator;");
    cacheMethod(env, ITERATOR_HAS_NEXT,
                JAVA_ITERATOR, "hasNext", "()Z");
    cacheMethod(env, ITERATOR_NEXT,
                JAVA_ITERATOR, "next", "()Ljava/lang/Object;");
    cacheMethod(env, ENTRY_GET_KEY,
                JAVA_MAP_ENTRY, "getKey", "()Ljava/lang/Object;");
    cacheMethod(env, ENTRY_GET_VALUE,
                JAVA_MAP_ENTRY, "getValue", "()Ljava/lang/Object;");
    cacheMethod(env, NUMBER_FLOAT_VALUE,
                JAVA_NUMBER, "floatValue", "()F");
    cacheMethod(env, NUMBER_INT_VALUE,
                JAVA_NUMBER, "intValue", "()I");
    cacheMethod(env, BOOLEAN_VALUE,
                JAVA_BOOLEAN, "booleanValue", "()Z");
    cacheMethod(env, SPARSE_QUERY_RESULT_CTOR,
                SPARSE_QUERY_RESULT, "<init>", "(IF)V");
    cacheMethod(env, THROWABLE_TO_STRING,
                JAVA_THROWABLE, "toString", "()Ljava/lang/String;");
}

void JniCachedRefs::release(JNIEnv* env) {
    for (auto& pair : cachedClasses) {
        env->DeleteGlobalRef(pair.second);
    }
    cachedClasses.clear();
    cachedMethods.clear();
}

void JniCachedRefs::cacheClass(JNIEnv* env, const char* className) {
    jclass local = env->FindClass(className);
    if (local == nullptr) {
        throw std::runtime_error(
            std::string("Failed to find class: ") + className);
    }
    auto global = static_cast<jclass>(env->NewGlobalRef(local));
    env->DeleteLocalRef(local);
    cachedClasses[className] = global;
}

void JniCachedRefs::cacheMethod(JNIEnv* env, const char* methodKey,
                                const char* classKey, const char* methodName,
                                const char* sig) {
    jclass cls = getClass(classKey);
    jmethodID mid = env->GetMethodID(cls, methodName, sig);
    if (mid == nullptr) {
        throw std::runtime_error(
            std::string("Failed to find method: ") + methodName +
            " on class " + classKey);
    }
    cachedMethods[methodKey] = mid;
}

// ============================================================================
// Exception helpers
// ============================================================================

void ThrowJavaException(JNIEnv* env, const char* classKey,
                        const char* message) {
    auto it = cachedRefs.cachedClasses.find(classKey);
    if (it != cachedRefs.cachedClasses.end()) {
        env->ThrowNew(it->second, message);
    } else {
        // Cache miss — fall back to FindClass so we never silently
        // swallow an exception (e.g. during JNI_OnLoad bootstrap).
        jclass exClass = env->FindClass(classKey);
        if (exClass != nullptr) {
            env->ThrowNew(exClass, message);
        }
    }
}

void CatchCppExceptionAndThrowJava(JNIEnv* env) {
    try {
        throw;
    } catch (const std::bad_alloc& e) {
        ThrowJavaException(env, JAVA_OOM_ERROR, e.what());
    } catch (const std::invalid_argument& e) {
        ThrowJavaException(env, JAVA_ILLEGAL_ARGUMENT, e.what());
    } catch (const std::exception& e) {
        ThrowJavaException(env, JAVA_EXCEPTION, e.what());
    } catch (...) {
        ThrowJavaException(env, JAVA_EXCEPTION,
                           "Unknown exception occurred");
    }
}

std::string DescribeAndClearPendingException(JNIEnv* env) {
    jthrowable pending = env->ExceptionOccurred();
    if (pending == nullptr) {
        return {};
    }
    // Must clear before making further JNI calls: most functions are not safe to
    // invoke with an exception pending.
    env->ExceptionClear();

    std::string description;
    try {
        jmethodID toString = cachedRefs.getMethod(THROWABLE_TO_STRING);
        auto text = static_cast<jstring>(env->CallObjectMethod(pending, toString));
        if (text != nullptr) {
            description = ScopedStringChars(env, text).toString();
            env->DeleteLocalRef(text);
        }
    } catch (const std::exception&) {
        // Cache miss (e.g. during JNI_OnLoad bootstrap) — fall through with an
        // empty description rather than masking the caller's own error.
    }
    // toString() itself can fail; don't leave that pending for the caller.
    env->ExceptionClear();
    env->DeleteLocalRef(pending);
    return description;
}

}  // namespace neural_search_jni
