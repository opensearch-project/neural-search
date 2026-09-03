/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OPENSEARCH_NEURALSEARCH_JNI_COMMON_H__
#define __OPENSEARCH_NEURALSEARCH_JNI_COMMON_H__

#include <cstdint>
#include <vector>

namespace neural_search_jni {

/**
 * Transfer sparse vector data from Java on-heap arrays into off-heap std::vectors.
 *
 * memoryAddresses is a 3-element array of pointers to std::vectors:
 *   [0] -> std::vector<int32_t>*  (indices / CSR indptr)
 *   [1] -> std::vector<int32_t>*  (tokens)
 *   [2] -> std::vector<float>*    (weights)
 *
 * If an address is 0, a new vector is heap-allocated and the address is written back.
 * Otherwise, data is appended to the existing vector.
 *
 * Every length may be 0; a call that transfers nothing leaves the vectors valid
 * for the next one.
 */
inline void transferVectors(
    int64_t* memoryAddresses,
    const int32_t* indices, int indicesLen,
    const int32_t* tokens, int tokensLen,
    const float* weights, int weightsLen
) {
    // --- indices (index 0) — CSR indptr ---
    // Each flush produces a relative indptr starting at 0.  When appending to
    // an existing vector we must (a) skip the redundant leading 0 and
    // (b) offset every value by the current cumulative nnz so the indptr
    // remains a valid, monotonically-increasing CSR array.
    std::vector<int32_t>* indicesVec;
    if (memoryAddresses[0] == 0) {
        indicesVec = new std::vector<int32_t>();
        memoryAddresses[0] = reinterpret_cast<int64_t>(indicesVec);
    } else {
        indicesVec = reinterpret_cast<std::vector<int32_t>*>(memoryAddresses[0]);
    }
    // Emptiness, not a null address, decides which branch applies: a first call
    // carrying no indptr at all leaves an allocated-but-empty vector behind, and
    // the append path below would then read back() from it and skip a leading 0
    // that was never stored.
    if (indicesVec->empty()) {
        indicesVec->insert(indicesVec->end(), indices, indices + indicesLen);
    } else {
        const int32_t offset = indicesVec->back();
        // Skip indices[0] (always 0) and offset the rest
        for (int i = 1; i < indicesLen; ++i) {
            indicesVec->push_back(indices[i] + offset);
        }
    }

    // --- tokens (index 1) ---
    std::vector<int32_t>* tokensVec;
    if (memoryAddresses[1] == 0) {
        tokensVec = new std::vector<int32_t>();
        memoryAddresses[1] = reinterpret_cast<int64_t>(tokensVec);
    } else {
        tokensVec = reinterpret_cast<std::vector<int32_t>*>(memoryAddresses[1]);
    }
    tokensVec->insert(tokensVec->end(), tokens, tokens + tokensLen);

    // --- weights (index 2) ---
    std::vector<float>* weightsVec;
    if (memoryAddresses[2] == 0) {
        weightsVec = new std::vector<float>();
        memoryAddresses[2] = reinterpret_cast<int64_t>(weightsVec);
    } else {
        weightsVec = reinterpret_cast<std::vector<float>*>(memoryAddresses[2]);
    }
    weightsVec->insert(weightsVec->end(), weights, weights + weightsLen);
}

}  // namespace neural_search_jni

#endif // __OPENSEARCH_NEURALSEARCH_JNI_COMMON_H__
