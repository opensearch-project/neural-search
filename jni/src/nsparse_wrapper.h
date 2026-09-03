/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OPENSEARCH_NEURALSEARCH_JNI_NSPARSE_WRAPPER_H__
#define __OPENSEARCH_NEURALSEARCH_JNI_NSPARSE_WRAPPER_H__

#include <jni.h>
#include <cstdint>
#include <string>
#include <map>

namespace neural_search_jni::nsparse_wrapper {

int64_t initIndex(int64_t numDocs, int dim,
                  const std::map<std::string, jobject>& parameters,
                  JNIEnv* env);

void insertToIndex(int64_t indexAddress, const int32_t* ids,
                   int numIds, int64_t indicesAddress,
                   int64_t tokensAddress, int64_t valuesAddress,
                   int threadCount);

void writeIndex(int64_t indexAddress, jobject output, JNIEnv* env);

// ioFlags is an nsparse::IndexIoFlag bitmask. Passing kUseMmap makes the nested
// SeismicIndex borrow its arrays from the file mapping instead of copying them
// onto the heap, so the OS page cache accounts for them.
int64_t loadIndex(const std::string& indexPath, int ioFlags = 0);

void queryIndex(int64_t indexAddress, const int32_t* tokens,
                const float* weights, int numTokens, int k,
                const std::map<std::string, jobject>& methodParameters,
                JNIEnv* env, float* distances, int32_t* labels);

void queryIndexWithFilter(int64_t indexAddress, const int32_t* tokens,
                          const float* weights, int numTokens, int k,
                          const std::map<std::string, jobject>& methodParameters,
                          const int64_t* filterIds, int numFilterIds,
                          int filterIdsType, JNIEnv* env,
                          float* distances, int32_t* labels);

void freeIndex(int64_t indexAddress);

}  // namespace neural_search_jni::nsparse_wrapper

#endif //__OPENSEARCH_NEURALSEARCH_JNI_NSPARSE_WRAPPER_H__
