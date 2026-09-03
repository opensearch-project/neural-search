/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

//
// A minimal, self-contained fake implementation of JNIEnv for unit testing the
// native JNI layer WITHOUT a running JVM.
//
// The production code (nsparse_wrapper, nsparse_stream_writer, the JNI entry
// points, jni_util) all dispatch through `env->...`, so to exercise the real
// logic we provide a JNINativeInterface_ function table backed by plain C++
// data structures.
//
// Design notes:
//   * FakeJniEnv begins with a `const JNINativeInterface_*` member so that a
//     pointer to it is a valid `JNIEnv*` (JNIEnv == const JNINativeInterface_*).
//     Static trampolines recover the FakeJniEnv via reinterpret_cast on the
//     first argument.
//   * jobject values are heap-allocated FakeObject instances, all owned by the
//     FakeJniEnv and freed on destruction. DeleteLocalRef therefore does NOT
//     free (avoids dangling refs stored inside object arrays); it only counts.
//   * jmethodID encodes the Java method being invoked (see Op), so the variadic
//     Call*Method trampolines can dispatch without a real method table.
//

#ifndef NEURAL_SEARCH_JNI_TESTS_FAKE_JNI_H
#define NEURAL_SEARCH_JNI_TESTS_FAKE_JNI_H

#include <jni.h>

#include <cstdarg>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace neural_search_jni::test {

// Identifies which Java method a jmethodID refers to. Encoded directly into the
// jmethodID pointer value by GetMethodID().
enum Op : intptr_t {
    OP_NONE = 0,
    OP_ENTRY_SET,
    OP_ITERATOR,
    OP_HAS_NEXT,
    OP_NEXT,
    OP_GET_KEY,
    OP_GET_VALUE,
    OP_FLOAT_VALUE,
    OP_INT_VALUE,
    OP_BOOL_VALUE,
    OP_CTOR,
    OP_WRITE_BYTES,
    OP_TO_STRING,
    OP_GET_FILE_POINTER,
};

enum Kind {
    K_STRING,
    K_NUMBER,
    K_BOOL,
    K_MAP,
    K_ENTRY_SET,
    K_ITERATOR,
    K_ENTRY,
    K_QUERY_RESULT,
    K_OBJ_ARRAY,
    K_INT_ARRAY,
    K_FLOAT_ARRAY,
    K_LONG_ARRAY,
    K_BYTE_ARRAY,
    K_OUTPUT,
    K_THROWABLE,
};

struct FakeObject {
    Kind kind;

    // K_STRING
    std::string str;
    // K_NUMBER
    double num = 0;
    // K_BOOL
    bool boolean = false;
    // K_MAP: ordered (key, value) entries
    std::vector<std::pair<std::string, jobject>> entries;
    // K_ENTRY_SET / K_ITERATOR
    jobject mapRef = nullptr;
    size_t iterIndex = 0;
    // K_ENTRY
    std::string entryKey;
    jobject entryValue = nullptr;
    // K_QUERY_RESULT
    int qrId = 0;
    float qrScore = 0;
    // K_OBJ_ARRAY
    std::vector<jobject> objArray;
    // primitive arrays
    std::vector<int32_t> ints;
    std::vector<float> floats;
    std::vector<int64_t> longs;
    std::vector<char> bytes;
    // K_OUTPUT: destination sink for writeBytes, plus the file offset the
    // wrapped IndexOutput is already at (IndexOutputWrapper.getFilePointer()).
    std::vector<char>* sink = nullptr;
    int64_t filePointer = 0;

    explicit FakeObject(Kind k) : kind(k) {}
};

class FakeJniEnv {
    // functions_ MUST be the very first member: env() hands out its address as
    // a JNIEnv*, and self() recovers the FakeJniEnv by casting that same
    // address back. Any member before it would break the round-trip.
    const JNINativeInterface_* functions_;
    JNINativeInterface_ table_;

public:
    FakeJniEnv() {
        std::memset(&table_, 0, sizeof(table_));
        table_.FindClass = &FindClass;
        table_.ExceptionOccurred = &ExceptionOccurred;
        table_.NewGlobalRef = &NewGlobalRef;
        table_.DeleteGlobalRef = &DeleteGlobalRef;
        table_.DeleteLocalRef = &DeleteLocalRef;
        table_.GetObjectClass = &GetObjectClass;
        table_.GetMethodID = &GetMethodID;
        table_.ThrowNew = &ThrowNew;
        table_.ExceptionCheck = &ExceptionCheck;
        table_.ExceptionClear = &ExceptionClear;
        table_.CallObjectMethodV = &CallObjectMethodV;
        table_.CallBooleanMethodV = &CallBooleanMethodV;
        table_.CallIntMethodV = &CallIntMethodV;
        table_.CallFloatMethodV = &CallFloatMethodV;
        table_.CallVoidMethodV = &CallVoidMethodV;
        table_.CallLongMethodV = &CallLongMethodV;
        table_.NewObjectV = &NewObjectV;
        table_.GetStringUTFChars = &GetStringUTFChars;
        table_.ReleaseStringUTFChars = &ReleaseStringUTFChars;
        table_.GetArrayLength = &GetArrayLength;
        table_.NewObjectArray = &NewObjectArray;
        table_.SetObjectArrayElement = &SetObjectArrayElement;
        table_.NewByteArray = &NewByteArray;
        table_.SetByteArrayRegion = &SetByteArrayRegion;
        table_.GetIntArrayElements = &GetIntArrayElements;
        table_.GetFloatArrayElements = &GetFloatArrayElements;
        table_.GetLongArrayElements = &GetLongArrayElements;
        table_.ReleaseIntArrayElements = &ReleaseIntArrayElements;
        table_.ReleaseFloatArrayElements = &ReleaseFloatArrayElements;
        table_.ReleaseLongArrayElements = &ReleaseLongArrayElements;
        functions_ = &table_;
    }

    JNIEnv* env() { return reinterpret_cast<JNIEnv*>(&functions_); }

    // ---- Object factory helpers (owned by this env) ----
    jobject makeString(const std::string& s) {
        auto* o = alloc(K_STRING);
        o->str = s;
        return obj(o);
    }
    jobject makeNumber(double v) {
        auto* o = alloc(K_NUMBER);
        o->num = v;
        return obj(o);
    }
    jobject makeBool(bool b) {
        auto* o = alloc(K_BOOL);
        o->boolean = b;
        return obj(o);
    }
    jobject makeMap() { return obj(alloc(K_MAP)); }

    void mapPut(jobject map, const std::string& key, jobject value) {
        fo(map)->entries.emplace_back(key, value);
    }

    jintArray makeIntArray(const std::vector<int32_t>& v) {
        auto* o = alloc(K_INT_ARRAY);
        o->ints = v;
        return reinterpret_cast<jintArray>(o);
    }
    jfloatArray makeFloatArray(const std::vector<float>& v) {
        auto* o = alloc(K_FLOAT_ARRAY);
        o->floats = v;
        return reinterpret_cast<jfloatArray>(o);
    }
    jlongArray makeLongArray(const std::vector<int64_t>& v) {
        auto* o = alloc(K_LONG_ARRAY);
        o->longs = v;
        return reinterpret_cast<jlongArray>(o);
    }
    jobject makeOutput(std::vector<char>* sink, int64_t filePointer = 0) {
        auto* o = alloc(K_OUTPUT);
        o->sink = sink;
        o->filePointer = filePointer;
        return obj(o);
    }

    static FakeObject* fo(jobject o) { return reinterpret_cast<FakeObject*>(o); }

    // Exposed test observability
    int throwCount = 0;
    std::string lastThrowMessage;
    int deleteLocalRefCount = 0;

    // Get<Type>ArrayElements / Release<Type>ArrayElements balance. A real JVM
    // either pins the array or hands back a copy; either way an unmatched Get
    // strands the resource. Tests assert this returns to zero even on throw paths.
    int arrayElementsOutstanding = 0;
    int arrayElementsReleased = 0;
    // Last release mode seen, so tests can assert copy-back (0) vs JNI_ABORT.
    jint lastReleaseMode = -1;

    // When set, the named method resolves to nullptr from GetMethodID, simulating
    // a Java class whose expected method is absent.
    std::string missingMethodName;

    // When non-empty, writeBytes raises a Java exception carrying this message,
    // exercising the pending-exception path in JniBufferedWriter::flushBuffer.
    std::string writeBytesFailureMessage;

    /** Simulate a pending Java exception with the given toString() text. */
    void setPendingException(const std::string& text) {
        auto* t = alloc(K_THROWABLE);
        t->str = text;
        pending_ = obj(t);
    }
    bool hasPendingException() const { return pending_ != nullptr; }

private:
    std::vector<std::unique_ptr<FakeObject>> pool_;
    jobject pending_ = nullptr;
    // Static class sentinel (non-null jclass for FindClass/GetObjectClass).
    static int classSentinel_;

    FakeObject* alloc(Kind k) {
        pool_.push_back(std::make_unique<FakeObject>(k));
        return pool_.back().get();
    }
    static jobject obj(FakeObject* o) { return reinterpret_cast<jobject>(o); }
    static FakeJniEnv* self(JNIEnv* e) { return reinterpret_cast<FakeJniEnv*>(e); }

    // ---- Trampolines ----
    static jclass FindClass(JNIEnv*, const char*) {
        return reinterpret_cast<jclass>(&classSentinel_);
    }
    static jobject NewGlobalRef(JNIEnv*, jobject o) { return o; }
    static void DeleteGlobalRef(JNIEnv*, jobject) {}
    static void DeleteLocalRef(JNIEnv* e, jobject) { self(e)->deleteLocalRefCount++; }
    static jclass GetObjectClass(JNIEnv*, jobject) {
        return reinterpret_cast<jclass>(&classSentinel_);
    }
    static jmethodID GetMethodID(JNIEnv* e, jclass, const char* name, const char*) {
        if (!self(e)->missingMethodName.empty() && self(e)->missingMethodName == name) {
            // A real JVM leaves a NoSuchMethodError pending in this case.
            self(e)->setPendingException("java.lang.NoSuchMethodError: " + std::string(name));
            return nullptr;
        }
        return reinterpret_cast<jmethodID>(opFromName(name));
    }
    static jint ThrowNew(JNIEnv* e, jclass, const char* msg) {
        self(e)->throwCount++;
        self(e)->lastThrowMessage = msg ? msg : "";
        return 0;
    }
    static jboolean ExceptionCheck(JNIEnv* e) {
        return self(e)->pending_ != nullptr ? JNI_TRUE : JNI_FALSE;
    }
    static jthrowable ExceptionOccurred(JNIEnv* e) {
        return reinterpret_cast<jthrowable>(self(e)->pending_);
    }
    static void ExceptionClear(JNIEnv* e) { self(e)->pending_ = nullptr; }

    static intptr_t opFromName(const char* name) {
        std::string n(name);
        if (n == "entrySet") return OP_ENTRY_SET;
        if (n == "iterator") return OP_ITERATOR;
        if (n == "hasNext") return OP_HAS_NEXT;
        if (n == "next") return OP_NEXT;
        if (n == "getKey") return OP_GET_KEY;
        if (n == "getValue") return OP_GET_VALUE;
        if (n == "floatValue") return OP_FLOAT_VALUE;
        if (n == "intValue") return OP_INT_VALUE;
        if (n == "booleanValue") return OP_BOOL_VALUE;
        if (n == "<init>") return OP_CTOR;
        if (n == "writeBytes") return OP_WRITE_BYTES;
        if (n == "toString") return OP_TO_STRING;
        if (n == "getFilePointer") return OP_GET_FILE_POINTER;
        return OP_NONE;
    }

    static jobject CallObjectMethodV(JNIEnv* e, jobject o, jmethodID mid, va_list) {
        FakeJniEnv* s = self(e);
        FakeObject* obj_in = fo(o);
        switch (reinterpret_cast<intptr_t>(mid)) {
            case OP_ENTRY_SET: {
                auto* es = s->alloc(K_ENTRY_SET);
                es->mapRef = o;
                return obj(es);
            }
            case OP_ITERATOR: {
                auto* it = s->alloc(K_ITERATOR);
                it->mapRef = obj_in->mapRef;  // point at underlying map
                it->iterIndex = 0;
                return obj(it);
            }
            case OP_NEXT: {
                FakeObject* map = fo(obj_in->mapRef);
                auto& pair = map->entries[obj_in->iterIndex++];
                auto* entry = s->alloc(K_ENTRY);
                entry->entryKey = pair.first;
                entry->entryValue = pair.second;
                return obj(entry);
            }
            case OP_GET_KEY:
                return s->makeString(obj_in->entryKey);
            case OP_GET_VALUE:
                return obj_in->entryValue;
            case OP_TO_STRING:
                return s->makeString(obj_in->str);
            default:
                return nullptr;
        }
    }
    static jboolean CallBooleanMethodV(JNIEnv*, jobject o, jmethodID mid, va_list) {
        FakeObject* obj_in = fo(o);
        switch (reinterpret_cast<intptr_t>(mid)) {
            case OP_HAS_NEXT: {
                FakeObject* map = fo(obj_in->mapRef);
                return obj_in->iterIndex < map->entries.size() ? JNI_TRUE : JNI_FALSE;
            }
            case OP_BOOL_VALUE:
                return obj_in->boolean ? JNI_TRUE : JNI_FALSE;
            default:
                return JNI_FALSE;
        }
    }
    static jint CallIntMethodV(JNIEnv*, jobject o, jmethodID, va_list) {
        return static_cast<jint>(fo(o)->num);
    }
    static jlong CallLongMethodV(JNIEnv*, jobject o, jmethodID mid, va_list) {
        if (reinterpret_cast<intptr_t>(mid) == OP_GET_FILE_POINTER) {
            return static_cast<jlong>(fo(o)->filePointer);
        }
        return 0;
    }
    static jfloat CallFloatMethodV(JNIEnv*, jobject o, jmethodID, va_list) {
        return static_cast<jfloat>(fo(o)->num);
    }
    static void CallVoidMethodV(JNIEnv* e, jobject o, jmethodID mid, va_list args) {
        if (reinterpret_cast<intptr_t>(mid) == OP_WRITE_BYTES) {
            jobject arr = va_arg(args, jobject);
            jint offset = va_arg(args, jint);
            jint length = va_arg(args, jint);
            if (!self(e)->writeBytesFailureMessage.empty()) {
                self(e)->setPendingException(self(e)->writeBytesFailureMessage);
                return;
            }
            FakeObject* out = fo(o);
            FakeObject* buf = fo(arr);
            if (out->sink != nullptr) {
                out->sink->insert(out->sink->end(), buf->bytes.begin() + offset,
                                  buf->bytes.begin() + offset + length);
            }
        }
    }
    static jobject NewObjectV(JNIEnv* e, jclass, jmethodID mid, va_list args) {
        if (reinterpret_cast<intptr_t>(mid) == OP_CTOR) {
            auto* qr = self(e)->alloc(K_QUERY_RESULT);
            qr->qrId = va_arg(args, jint);
            qr->qrScore = static_cast<float>(va_arg(args, double));  // float promoted
            return obj(qr);
        }
        return nullptr;
    }
    static const char* GetStringUTFChars(JNIEnv*, jstring s, jboolean* isCopy) {
        if (isCopy != nullptr) *isCopy = JNI_FALSE;
        return fo(reinterpret_cast<jobject>(s))->str.c_str();
    }
    static void ReleaseStringUTFChars(JNIEnv*, jstring, const char*) {}
    static jsize GetArrayLength(JNIEnv*, jarray a) {
        FakeObject* o = fo(reinterpret_cast<jobject>(a));
        switch (o->kind) {
            case K_INT_ARRAY: return static_cast<jsize>(o->ints.size());
            case K_FLOAT_ARRAY: return static_cast<jsize>(o->floats.size());
            case K_LONG_ARRAY: return static_cast<jsize>(o->longs.size());
            case K_BYTE_ARRAY: return static_cast<jsize>(o->bytes.size());
            case K_OBJ_ARRAY: return static_cast<jsize>(o->objArray.size());
            default: return 0;
        }
    }
    static jobjectArray NewObjectArray(JNIEnv* e, jsize len, jclass, jobject init) {
        auto* o = self(e)->alloc(K_OBJ_ARRAY);
        o->objArray.assign(len, init);
        return reinterpret_cast<jobjectArray>(o);
    }
    static void SetObjectArrayElement(JNIEnv*, jobjectArray a, jsize i, jobject v) {
        fo(reinterpret_cast<jobject>(a))->objArray[i] = v;
    }
    static jbyteArray NewByteArray(JNIEnv* e, jsize len) {
        auto* o = self(e)->alloc(K_BYTE_ARRAY);
        o->bytes.resize(len);
        return reinterpret_cast<jbyteArray>(o);
    }
    static void SetByteArrayRegion(JNIEnv*, jbyteArray a, jsize start, jsize len,
                                   const jbyte* buf) {
        FakeObject* o = fo(reinterpret_cast<jobject>(a));
        std::memcpy(o->bytes.data() + start, buf, len);
    }
    static jint* GetIntArrayElements(JNIEnv* e, jintArray a, jboolean* isCopy) {
        if (isCopy != nullptr) *isCopy = JNI_FALSE;
        self(e)->arrayElementsOutstanding++;
        return reinterpret_cast<jint*>(fo(reinterpret_cast<jobject>(a))->ints.data());
    }
    static jfloat* GetFloatArrayElements(JNIEnv* e, jfloatArray a, jboolean* isCopy) {
        if (isCopy != nullptr) *isCopy = JNI_FALSE;
        self(e)->arrayElementsOutstanding++;
        return reinterpret_cast<jfloat*>(fo(reinterpret_cast<jobject>(a))->floats.data());
    }
    static jlong* GetLongArrayElements(JNIEnv* e, jlongArray a, jboolean* isCopy) {
        if (isCopy != nullptr) *isCopy = JNI_FALSE;
        self(e)->arrayElementsOutstanding++;
        return reinterpret_cast<jlong*>(fo(reinterpret_cast<jobject>(a))->longs.data());
    }
    static void noteRelease(JNIEnv* e, jint mode) {
        self(e)->arrayElementsOutstanding--;
        self(e)->arrayElementsReleased++;
        self(e)->lastReleaseMode = mode;
    }
    static void ReleaseIntArrayElements(JNIEnv* e, jintArray, jint*, jint mode) {
        noteRelease(e, mode);
    }
    static void ReleaseFloatArrayElements(JNIEnv* e, jfloatArray, jfloat*, jint mode) {
        noteRelease(e, mode);
    }
    static void ReleaseLongArrayElements(JNIEnv* e, jlongArray, jlong*, jint mode) {
        noteRelease(e, mode);
    }
};

inline int FakeJniEnv::classSentinel_ = 0;

}  // namespace neural_search_jni::test

#endif  // NEURAL_SEARCH_JNI_TESTS_FAKE_JNI_H
