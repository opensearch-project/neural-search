/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

//
// Unit tests for common.h::transferVectors — the CSR accumulation routine that
// copies sparse-vector data from Java on-heap arrays into off-heap
// std::vectors, appending across multiple flushes.
//
// These tests own the off-heap vectors: transferVectors allocates on the first
// call (memoryAddresses == 0) and appends thereafter. We free them at the end
// of each test so the suite is clean under ASan/valgrind.
//

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common.h"

using neural_search_jni::transferVectors;

namespace {

// RAII owner for the three off-heap vectors addressed by memoryAddresses.
struct OffHeap {
    int64_t addr[3] = {0, 0, 0};

    std::vector<int32_t>* indices() {
        return reinterpret_cast<std::vector<int32_t>*>(addr[0]);
    }
    std::vector<int32_t>* tokens() {
        return reinterpret_cast<std::vector<int32_t>*>(addr[1]);
    }
    std::vector<float>* values() {
        return reinterpret_cast<std::vector<float>*>(addr[2]);
    }

    ~OffHeap() {
        delete indices();
        delete tokens();
        delete values();
    }
};

}  // namespace

TEST(TransferVectorsTest, FirstCallAllocatesAndCopies) {
    OffHeap off;
    std::vector<int32_t> indptr = {0, 2, 3};  // 2 docs: nnz 2 then 1
    std::vector<int32_t> tokens = {5, 9, 7};
    std::vector<float> weights = {1.0f, 2.0f, 3.0f};

    transferVectors(off.addr, indptr.data(), indptr.size(), tokens.data(),
                    tokens.size(), weights.data(), weights.size());

    ASSERT_NE(off.addr[0], 0);
    ASSERT_NE(off.addr[1], 0);
    ASSERT_NE(off.addr[2], 0);

    EXPECT_EQ(*off.indices(), (std::vector<int32_t>{0, 2, 3}));
    EXPECT_EQ(*off.tokens(), (std::vector<int32_t>{5, 9, 7}));
    EXPECT_EQ(*off.values(), (std::vector<float>{1.0f, 2.0f, 3.0f}));
}

TEST(TransferVectorsTest, SecondCallAppendsAndOffsetsIndptr) {
    OffHeap off;
    // Flush 1: indptr [0,2,3], 3 nnz.
    std::vector<int32_t> indptr1 = {0, 2, 3};
    std::vector<int32_t> tok1 = {5, 9, 7};
    std::vector<float> w1 = {1.0f, 2.0f, 3.0f};
    transferVectors(off.addr, indptr1.data(), indptr1.size(), tok1.data(),
                    tok1.size(), w1.data(), w1.size());

    // Flush 2: a relative indptr [0,1,3] — the leading 0 must be dropped and
    // the rest offset by the current cumulative nnz (3).
    std::vector<int32_t> indptr2 = {0, 1, 3};
    std::vector<int32_t> tok2 = {2, 4, 6};
    std::vector<float> w2 = {4.0f, 5.0f, 6.0f};
    transferVectors(off.addr, indptr2.data(), indptr2.size(), tok2.data(),
                    tok2.size(), w2.data(), w2.size());

    // Expected merged CSR: [0,2,3] then 4(=1+3), 6(=3+3).
    EXPECT_EQ(*off.indices(), (std::vector<int32_t>{0, 2, 3, 4, 6}));
    EXPECT_EQ(*off.tokens(), (std::vector<int32_t>{5, 9, 7, 2, 4, 6}));
    EXPECT_EQ(*off.values(),
              (std::vector<float>{1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f}));

    // The merged indptr must remain monotonically non-decreasing.
    const auto& idx = *off.indices();
    for (size_t i = 1; i < idx.size(); ++i) {
        EXPECT_LE(idx[i - 1], idx[i]);
    }
}

TEST(TransferVectorsTest, ManyFlushesProduceValidCsr) {
    OffHeap off;
    // Simulate 5 flushes, each adding one doc with 2 nnz.
    int32_t expectedNnz = 0;
    for (int flush = 0; flush < 5; ++flush) {
        std::vector<int32_t> indptr = {0, 2};
        std::vector<int32_t> tok = {flush, flush + 1};
        std::vector<float> w = {static_cast<float>(flush), static_cast<float>(flush)};
        transferVectors(off.addr, indptr.data(), indptr.size(), tok.data(),
                        tok.size(), w.data(), w.size());
        expectedNnz += 2;
    }

    const auto& idx = *off.indices();
    // 5 docs -> 6 indptr entries [0,2,4,6,8,10].
    ASSERT_EQ(idx.size(), 6u);
    EXPECT_EQ(idx.front(), 0);
    EXPECT_EQ(idx.back(), expectedNnz);
    EXPECT_EQ(static_cast<int32_t>(off.tokens()->size()), expectedNnz);
    EXPECT_EQ(static_cast<int32_t>(off.values()->size()), expectedNnz);
    for (size_t i = 1; i < idx.size(); ++i) {
        EXPECT_EQ(idx[i] - idx[i - 1], 2);
    }
}

TEST(TransferVectorsTest, EmptyAppendDoesNotCorruptIndptr) {
    OffHeap off;
    std::vector<int32_t> indptr1 = {0, 3};
    std::vector<int32_t> tok1 = {1, 2, 3};
    std::vector<float> w1 = {1.0f, 1.0f, 1.0f};
    transferVectors(off.addr, indptr1.data(), indptr1.size(), tok1.data(),
                    tok1.size(), w1.data(), w1.size());

    // A degenerate flush with only the leading 0 (no docs) appends nothing.
    std::vector<int32_t> indptrEmpty = {0};
    transferVectors(off.addr, indptrEmpty.data(), indptrEmpty.size(), nullptr, 0,
                    nullptr, 0);

    EXPECT_EQ(*off.indices(), (std::vector<int32_t>{0, 3}));
    EXPECT_EQ(off.tokens()->size(), 3u);
}

TEST(TransferVectorsTest, ZeroLengthFirstCallLeavesBuffersUsable) {
    OffHeap off;
    // A first call carrying nothing still allocates, so the next call sees a
    // non-null address pointing at an empty vector. Reading back() from it would
    // be undefined, and treating the next indptr as relative would drop its
    // leading 0 and leave the CSR one entry short of its doc count.
    transferVectors(off.addr, nullptr, 0, nullptr, 0, nullptr, 0);
    ASSERT_NE(off.addr[0], 0);
    EXPECT_TRUE(off.indices()->empty());

    std::vector<int32_t> indptr = {0, 3, 7};  // 2 docs
    std::vector<int32_t> tok = {1, 2, 3, 4, 5, 6, 7};
    std::vector<float> w(7, 1.0f);
    transferVectors(off.addr, indptr.data(), indptr.size(), tok.data(),
                    tok.size(), w.data(), w.size());

    EXPECT_EQ(*off.indices(), (std::vector<int32_t>{0, 3, 7}));
    EXPECT_EQ(off.tokens()->size(), 7u);
}

TEST(TransferVectorsTest, LeadingZeroOnlyFirstCallDoesNotDoubleCount) {
    OffHeap off;
    // The boundary next to the case above: {0} is already a valid empty CSR, so
    // the following flush is a genuine append and its leading 0 is redundant.
    std::vector<int32_t> indptrZero = {0};
    transferVectors(off.addr, indptrZero.data(), indptrZero.size(), nullptr, 0,
                    nullptr, 0);
    EXPECT_EQ(*off.indices(), (std::vector<int32_t>{0}));

    std::vector<int32_t> indptr = {0, 3, 7};
    std::vector<int32_t> tok = {1, 2, 3, 4, 5, 6, 7};
    std::vector<float> w(7, 1.0f);
    transferVectors(off.addr, indptr.data(), indptr.size(), tok.data(),
                    tok.size(), w.data(), w.size());

    EXPECT_EQ(*off.indices(), (std::vector<int32_t>{0, 3, 7}));
}
