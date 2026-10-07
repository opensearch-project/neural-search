/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

//
// Unit tests for jni_util.{h,cpp}: the class/method cache and the C++ ->
// Java exception translation helpers. Exercised against the fake JNIEnv.
//

#include <gtest/gtest.h>

#include <stdexcept>
#include <new>

#include "fake_jni.h"
#include "jni_util.h"

using namespace neural_search_jni;
using neural_search_jni::test::FakeJniEnv;

// A fresh, initialized cache for each test, released on teardown so we never
// leak global refs across tests.
class JniCacheTest : public ::testing::Test {
protected:
    FakeJniEnv fake;

    void SetUp() override { cachedRefs.init(fake.env()); }
    void TearDown() override { cachedRefs.release(fake.env()); }
};

TEST_F(JniCacheTest, InitPopulatesClassesAndMethods) {
    // Every class key registered in init() must be retrievable.
    EXPECT_NE(cachedRefs.getClass(JAVA_MAP), nullptr);
    EXPECT_NE(cachedRefs.getClass(SPARSE_QUERY_RESULT), nullptr);
    EXPECT_NE(cachedRefs.getClass(JAVA_OOM_ERROR), nullptr);

    // Every method key too.
    EXPECT_NE(cachedRefs.getMethod(MAP_ENTRY_SET), nullptr);
    EXPECT_NE(cachedRefs.getMethod(SPARSE_QUERY_RESULT_CTOR), nullptr);
    EXPECT_NE(cachedRefs.getMethod(NUMBER_FLOAT_VALUE), nullptr);
}

TEST_F(JniCacheTest, GetClassUnknownKeyThrows) {
    EXPECT_THROW(cachedRefs.getClass("does/not/Exist"), std::runtime_error);
}

TEST_F(JniCacheTest, GetMethodUnknownKeyThrows) {
    EXPECT_THROW(cachedRefs.getMethod("NO_SUCH_METHOD"), std::runtime_error);
}

TEST_F(JniCacheTest, ReleaseClearsCacheAndIsIdempotent) {
    cachedRefs.release(fake.env());
    EXPECT_THROW(cachedRefs.getClass(JAVA_MAP), std::runtime_error);
    // A second release must not crash (maps already empty).
    cachedRefs.release(fake.env());
    // Re-init so TearDown's release has something valid to operate on.
    cachedRefs.init(fake.env());
}

// ---- Exception translation -------------------------------------------------

TEST_F(JniCacheTest, ThrowJavaExceptionUsesCachedClass) {
    ThrowJavaException(fake.env(), JAVA_ILLEGAL_ARGUMENT, "bad arg");
    EXPECT_EQ(fake.throwCount, 1);
    EXPECT_EQ(fake.lastThrowMessage, "bad arg");
}

TEST_F(JniCacheTest, ThrowJavaExceptionFallsBackToFindClassOnCacheMiss) {
    // A class key that was never cached still results in a thrown exception via
    // the FindClass fallback path.
    ThrowJavaException(fake.env(), "java/lang/IllegalStateException", "boom");
    EXPECT_EQ(fake.throwCount, 1);
    EXPECT_EQ(fake.lastThrowMessage, "boom");
}

TEST_F(JniCacheTest, CatchBadAllocMapsToOom) {
    try {
        throw std::bad_alloc();
    } catch (...) {
        CatchCppExceptionAndThrowJava(fake.env());
    }
    EXPECT_EQ(fake.throwCount, 1);
}

TEST_F(JniCacheTest, CatchInvalidArgumentMapsAndPreservesMessage) {
    try {
        throw std::invalid_argument("bad param");
    } catch (...) {
        CatchCppExceptionAndThrowJava(fake.env());
    }
    EXPECT_EQ(fake.throwCount, 1);
    EXPECT_EQ(fake.lastThrowMessage, "bad param");
}

TEST_F(JniCacheTest, CatchRuntimeErrorMapsToException) {
    try {
        throw std::runtime_error("oops");
    } catch (...) {
        CatchCppExceptionAndThrowJava(fake.env());
    }
    EXPECT_EQ(fake.throwCount, 1);
    EXPECT_EQ(fake.lastThrowMessage, "oops");
}

TEST_F(JniCacheTest, CatchUnknownExceptionMapsToGenericMessage) {
    try {
        throw 42;  // non-std::exception
    } catch (...) {
        CatchCppExceptionAndThrowJava(fake.env());
    }
    EXPECT_EQ(fake.throwCount, 1);
    EXPECT_EQ(fake.lastThrowMessage, "Unknown exception occurred");
}

// ---- Pending-exception handling --------------------------------------------

TEST_F(JniCacheTest, DescribeAndClearReturnsEmptyWhenNothingPending) {
    EXPECT_TRUE(DescribeAndClearPendingException(fake.env()).empty());
}

TEST_F(JniCacheTest, DescribeAndClearReturnsMessageAndClears) {
    fake.setPendingException("java.io.IOException: disk went away");
    ASSERT_TRUE(fake.hasPendingException());

    std::string described = DescribeAndClearPendingException(fake.env());

    EXPECT_EQ(described, "java.io.IOException: disk went away");
    // Leaving an exception pending makes every later JNI call in the frame
    // undefined, so it must be cleared before returning to the caller.
    EXPECT_FALSE(fake.hasPendingException());
}

// ---- RAII guards -----------------------------------------------------------

TEST_F(JniCacheTest, ScopedArrayElementsReleasesOnScopeExit) {
    jintArray ints = fake.makeIntArray({1, 2, 3});

    {
        ScopedIntArray scoped(fake.env(), ints);
        ASSERT_NE(scoped.data(), nullptr);
        EXPECT_EQ(scoped.length(), 3);
        EXPECT_EQ(fake.arrayElementsOutstanding, 1);
    }

    EXPECT_EQ(fake.arrayElementsOutstanding, 0);
    EXPECT_EQ(fake.lastReleaseMode, JNI_ABORT);
}

TEST_F(JniCacheTest, ScopedArrayElementsReleasesWhenScopeUnwinds) {
    jfloatArray floats = fake.makeFloatArray({1.0f, 2.0f});

    try {
        ScopedFloatArray scoped(fake.env(), floats);
        EXPECT_EQ(fake.arrayElementsOutstanding, 1);
        throw std::runtime_error("boom");
    } catch (const std::runtime_error&) {
        // swallowed
    }

    EXPECT_EQ(fake.arrayElementsOutstanding, 0)
        << "array elements leaked when the scope unwound";
}

TEST_F(JniCacheTest, ScopedArrayElementsHonorsCopyBackReleaseMode) {
    jlongArray longs = fake.makeLongArray({0, 0, 0});

    {
        ScopedLongArray scoped(fake.env(), longs, /*releaseMode=*/0);
        scoped.data()[0] = 42;
    }

    EXPECT_EQ(fake.lastReleaseMode, 0);
    EXPECT_EQ(FakeJniEnv::fo(reinterpret_cast<jobject>(longs))->longs[0], 42);
}

TEST_F(JniCacheTest, ScopedArrayElementsToleratesNullArray) {
    ScopedIntArray scoped(fake.env(), nullptr);
    EXPECT_EQ(scoped.data(), nullptr);
    EXPECT_EQ(scoped.length(), 0);
    EXPECT_EQ(fake.arrayElementsOutstanding, 0);
}

TEST_F(JniCacheTest, ScopedLocalRefDeletesOnScopeExit) {
    int before = fake.deleteLocalRefCount;
    {
        ScopedLocalRef ref(fake.env(), fake.makeString("hello"));
        EXPECT_NE(ref.get(), nullptr);
        EXPECT_EQ(fake.deleteLocalRefCount, before);
    }
    EXPECT_EQ(fake.deleteLocalRefCount, before + 1);
}

TEST_F(JniCacheTest, ScopedLocalRefNullIsNoop) {
    int before = fake.deleteLocalRefCount;
    { ScopedLocalRef ref(fake.env(), nullptr); }
    EXPECT_EQ(fake.deleteLocalRefCount, before);
}

TEST_F(JniCacheTest, ScopedLocalRefMoveTransfersOwnership) {
    int before = fake.deleteLocalRefCount;
    {
        ScopedLocalRef first(fake.env(), fake.makeString("x"));
        ScopedLocalRef second(std::move(first));
        EXPECT_NE(second.get(), nullptr);
    }
    // Exactly one delete, from the moved-to guard.
    EXPECT_EQ(fake.deleteLocalRefCount, before + 1);
}

TEST_F(JniCacheTest, ScopedStringCharsConvertsAndReleases) {
    auto str = static_cast<jstring>(fake.makeString("native"));
    ScopedStringChars chars(fake.env(), str);
    EXPECT_STREQ(chars.get(), "native");
    EXPECT_EQ(chars.toString(), "native");
}

TEST_F(JniCacheTest, ScopedStringCharsToleratesNull) {
    ScopedStringChars chars(fake.env(), nullptr);
    EXPECT_EQ(chars.get(), nullptr);
    EXPECT_TRUE(chars.toString().empty());
}
