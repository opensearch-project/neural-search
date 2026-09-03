/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

#include "nsparse_stream_writer.h"

#include <cstring>
#include <stdexcept>

#include "jni_util.h"

namespace neural_search_jni {

// ============================================================================
// JniBufferedWriter
// ============================================================================

static constexpr size_t DEFAULT_BUFFER_SIZE = 64 * 1024;  // 64 KB

JniBufferedWriter::JniBufferedWriter(JNIEnv* env, jobject output)
    : env_(env),
      output_(output),
      capacity_(DEFAULT_BUFFER_SIZE),
      buffer_(DEFAULT_BUFFER_SIZE),
      pos_(0),
      start_offset_(0) {
    ScopedLocalRef cls(env_, env_->GetObjectClass(output_));

    // Look up IndexOutputWrapper.writeBytes(byte[], int, int)
    write_method_ = env_->GetMethodID(static_cast<jclass>(cls.get()), "writeBytes", "([BII)V");
    if (write_method_ == nullptr) {
        env_->ExceptionClear();
        throw std::runtime_error(
            "IndexOutputWrapper.writeBytes(byte[], int, int) not found");
    }

    // Where our first byte lands in the file. nsparse's alignment padding is
    // computed from this, not from a count of bytes we have written.
    jmethodID file_pointer_method =
        env_->GetMethodID(static_cast<jclass>(cls.get()), "getFilePointer", "()J");
    if (file_pointer_method == nullptr) {
        env_->ExceptionClear();
        throw std::runtime_error("IndexOutputWrapper.getFilePointer() not found");
    }
    jlong offset = env_->CallLongMethod(output_, file_pointer_method);
    std::string cause = DescribeAndClearPendingException(env_);
    if (!cause.empty()) {
        throw std::runtime_error(
            "IndexOutputWrapper.getFilePointer() failed: " + cause);
    }
    if (offset < 0) {
        throw std::runtime_error("IndexOutputWrapper.getFilePointer() returned a negative offset");
    }
    start_offset_ = static_cast<size_t>(offset);
}

void JniBufferedWriter::write(const void* ptr, size_t bytes) {
    auto src = static_cast<const char*>(ptr);
    size_t remaining = bytes;

    while (remaining > 0) {
        size_t space = capacity_ - pos_;
        size_t toCopy = std::min(remaining, space);

        std::memcpy(buffer_.data() + pos_, src, toCopy);
        pos_ += toCopy;
        src += toCopy;
        remaining -= toCopy;

        if (pos_ == capacity_) {
            flushBuffer(capacity_);
        }
    }
}

void JniBufferedWriter::flush() {
    if (pos_ > 0) {
        flushBuffer(pos_);
    }
}

void JniBufferedWriter::flushBuffer(size_t length) {
    // Create a temporary Java byte[], copy native data in, call writeBytes
    jbyteArray jbuf = env_->NewByteArray(static_cast<jint>(length));
    if (jbuf == nullptr) {
        // The JVM could not allocate; an OutOfMemoryError is already pending.
        env_->ExceptionClear();
        throw std::bad_alloc();
    }
    ScopedLocalRef bufRef(env_, jbuf);

    env_->SetByteArrayRegion(
        jbuf, 0, static_cast<jint>(length),
        reinterpret_cast<const jbyte*>(buffer_.data()));

    env_->CallVoidMethod(
        output_, write_method_, jbuf, 0, static_cast<jint>(length));

    // Carry the ORIGINAL Java failure into the message. IndexOutput.writeBytes
    // throws IOException for the cases that actually matter here (disk full, a
    // closed directory); reporting only "writeBytes failed" loses the cause and
    // makes a failed segment write undiagnosable from the logs.
    std::string cause = DescribeAndClearPendingException(env_);
    if (!cause.empty()) {
        throw std::runtime_error(
            "IndexOutputWrapper.writeBytes failed: " + cause);
    }

    pos_ = 0;
}

// ============================================================================
// NsparseStreamWriter
// ============================================================================

NsparseStreamWriter::NsparseStreamWriter(
    std::unique_ptr<JniBufferedWriter> writer)
    : writer_(std::move(writer)), bytes_written_(writer_->startOffset()) {}

void NsparseStreamWriter::write(void* ptr, size_t size, size_t nitems) {
    writer_->write(ptr, size * nitems);
    bytes_written_ += size * nitems;
}

size_t NsparseStreamWriter::pos() const { return bytes_written_; }

void NsparseStreamWriter::close() {
    writer_->flush();
}

}  // namespace neural_search_jni
