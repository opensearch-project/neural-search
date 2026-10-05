/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

#ifndef NSPARSE_STREAM_WRITER_H
#define NSPARSE_STREAM_WRITER_H

#include <jni.h>

#include <cstddef>
#include <memory>
#include <vector>

#include "nsparse/io/io.h"

namespace neural_search_jni {

/**
 * Manages a native-side buffer and flushes to a Java IndexOutputWrapper.
 *
 * On flush, creates a temporary Java byte[], copies native data into it via
 * SetByteArrayRegion, then calls IndexOutputWrapper.writeBytes(byte[], int, int)
 * to write directly to Lucene's IndexOutput.
 */
class JniBufferedWriter {
public:
    JniBufferedWriter(JNIEnv* env, jobject output);

    /**
     * Append data to the native buffer, flushing to Java when full.
     */
    void write(const void* ptr, size_t bytes);

    /**
     * Flush any remaining buffered data to Java.
     */
    void flush();

    /**
     * Byte offset in the output file where this writer's first byte lands, read
     * from IndexOutputWrapper.getFilePointer(). Zero when the native payload
     * starts the file.
     */
    size_t startOffset() const { return start_offset_; }

private:
    void flushBuffer(size_t length);

    JNIEnv* env_;
    jobject output_;            // IndexOutputWrapper instance
    jmethodID write_method_;    // IndexOutputWrapper.writeBytes(byte[], int, int)
    size_t start_offset_;       // IndexOutputWrapper.getFilePointer() at construction
    size_t capacity_;           // Buffer size / flush threshold
    std::vector<char> buffer_;  // Pre-allocated native buffer
    size_t pos_;                // Current write position in buffer
};

/**
 * nsparse::IOWriter implementation that streams serialized index data
 * to a Java IndexOutputWrapper via JniBufferedWriter.
 */
class NsparseStreamWriter : public nsparse::IOWriter {
public:
    explicit NsparseStreamWriter(std::unique_ptr<JniBufferedWriter> writer);
    void write(void* ptr, size_t size, size_t nitems) override;
    size_t pos() const override;
    void close() override;

private:
    std::unique_ptr<JniBufferedWriter> writer_;
    // Counted here rather than read off JniBufferedWriter, whose pos_ is an
    // offset into the flush buffer and rewinds on every flush.
    //
    // Seeded from the IndexOutput's file pointer rather than starting at zero:
    // nsparse pads each serialized array to its element alignment from pos(), and
    // the mmap reader recomputes that padding from the array's offset *in the
    // file*. A writer that counts only its own bytes agrees with the reader only
    // while the payload happens to start at offset 0.
    size_t bytes_written_ = 0;
};

}  // namespace neural_search_jni

#endif  // NSPARSE_STREAM_WRITER_H
