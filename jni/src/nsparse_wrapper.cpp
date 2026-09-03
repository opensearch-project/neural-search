/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "nsparse_wrapper.h"

#include <jni.h>
#include <omp.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "jni_util.h"
#include "nsparse/disk_seismic_index.h"
#include "nsparse/id_selector.h"
#include "nsparse/index.h"
#include "nsparse/index_factory.h"
#include "nsparse/io/index_io.h"
#include "nsparse/seismic_index.h"
#include "nsparse/seismic_scalar_quantized_index.h"
#include "nsparse/types.h"
#include "nsparse_stream_writer.h"

namespace neural_search_jni::nsparse_wrapper {

namespace {

/**
 * Extract a std::string from a Java Map<String, Object> entry value.
 * Assumes the value is a java.lang.String.
 */
std::string jstring_to_string(JNIEnv* env, jobject jstr) {
    return ScopedStringChars(env, static_cast<jstring>(jstr)).toString();
}

/**
 * Extract a float from a Java Number object.
 */
float jobject_to_float(JNIEnv* env, jobject obj) {
    return env->CallFloatMethod(obj,
                                neural_search_jni::cachedRefs.getMethod(
                                    neural_search_jni::NUMBER_FLOAT_VALUE));
}

/**
 * Extract an int from a Java Number object.
 */
int jobject_to_int(JNIEnv* env, jobject obj) {
    return env->CallIntMethod(obj, neural_search_jni::cachedRefs.getMethod(
                                       neural_search_jni::NUMBER_INT_VALUE));
}

/**
 * Extract a bool from a Java Boolean object.
 */
bool jobject_to_bool(JNIEnv* env, jobject obj) {
    return env->CallBooleanMethod(obj, neural_search_jni::cachedRefs.getMethod(
                                           neural_search_jni::BOOLEAN_VALUE));
}

/**
 * Build the index_factory description string from Java parameters map.
 *
 * Expected keys:
 *   "idmap"     -> Boolean (if true, wraps with IDMapIndex)
 *   "index"     -> String  (e.g. "seismic_sq", "seismic", "brutal")
 *   "quantizer" -> String  (e.g. "8bit", "16bit")
 *   "vmin"      -> Number  (float)
 *   "vmax"      -> Number  (float)
 *   "lambda"    -> Number  (int)
 *   "beta"      -> Number  (float -> int)
 *   "alpha"     -> Number  (float)
 *
 * Produces e.g.:
 * "idmap,seismic_sq,quantizer=8bit|vmin=0.0|vmax=1.0|lambda=10|beta=5|alpha=0.4"
 */
std::string buildDescription(const std::map<std::string, jobject>& params,
                             JNIEnv* env) {
    std::string desc;

    // Check for idmap wrapper
    auto it = params.find("idmap");
    if (it != params.end() && jobject_to_bool(env, it->second)) {
        desc += "idmap,";
    }

    // Index type
    it = params.find("index");
    if (it == params.end()) {
        throw std::invalid_argument("Missing required parameter 'index'");
    }
    desc += jstring_to_string(env, it->second);

    // Collect construction parameters
    std::string paramStr;
    auto appendParam = [&](const std::string& key, const std::string& value) {
        if (!paramStr.empty()) paramStr += "|";
        paramStr += key + "=" + value;
    };

    it = params.find("quantizer");
    if (it != params.end()) {
        appendParam("quantizer", jstring_to_string(env, it->second));
    }
    it = params.find("vmin");
    if (it != params.end()) {
        appendParam("vmin", std::to_string(jobject_to_float(env, it->second)));
    }
    it = params.find("vmax");
    if (it != params.end()) {
        appendParam("vmax", std::to_string(jobject_to_float(env, it->second)));
    }
    it = params.find("lambda");
    if (it != params.end()) {
        appendParam("lambda", std::to_string(jobject_to_int(env, it->second)));
    }
    it = params.find("beta");
    if (it != params.end()) {
        // beta comes as float from Java (clusterRatio * nPostings) but
        // index_factory expects int
        appendParam("beta", std::to_string(static_cast<int>(
                                jobject_to_float(env, it->second))));
    }
    it = params.find("alpha");
    if (it != params.end()) {
        appendParam("alpha", std::to_string(jobject_to_float(env, it->second)));
    }

    if (!paramStr.empty()) {
        desc += "," + paramStr;
    }

    return desc;
}

/**
 * Build the SearchParameters subtype the target index expects, from the Java map.
 *
 * The subtype is inferred from which keys are present, because the index type is
 * not visible here:
 *   "k_prime"      -> DiskSeismicSearchParameters (disk_seismic)
 *   "vmin"+"vmax"  -> SeismicSQSearchParameters   (seismic_sq)
 *   otherwise      -> SeismicSearchParameters
 *
 * Each index dynamic_casts to the type it wants and ignores the rest, so an
 * over-specific subtype is safe: DiskSeismicSearchParameters derives from
 * SeismicSearchParameters, and a plain SeismicIndex still reads cut/heap_factor
 * off it.
 */
std::unique_ptr<nsparse::SearchParameters> buildSearchParameters(
    const std::map<std::string, jobject>& params, JNIEnv* env) {
    int cut = 10;
    float heapFactor = 1.0f;

    auto it = params.find("cut");
    if (it != params.end()) {
        cut = jobject_to_int(env, it->second);
    }
    it = params.find("heap_factor");
    if (it != params.end()) {
        heapFactor = jobject_to_float(env, it->second);
    }

    // k_prime is the block budget DiskSeismicIndex reads, and it only exists on
    // DiskSeismicSearchParameters. Without this branch the index silently falls
    // back to kDefaultBlockBudget, leaving its main recall/latency knob unusable
    // from Java. DiskSeismicIndex ignores heap_factor by design.
    auto kPrimeIt = params.find("k_prime");
    if (kPrimeIt != params.end()) {
        int kPrime = jobject_to_int(env, kPrimeIt->second);
        return std::make_unique<nsparse::DiskSeismicSearchParameters>(cut, kPrime);
    }

    // If vmin/vmax are present, this is a SeismicSQ query
    auto vminIt = params.find("vmin");
    auto vmaxIt = params.find("vmax");
    if (vminIt != params.end() && vmaxIt != params.end()) {
        float vmin = jobject_to_float(env, vminIt->second);
        float vmax = jobject_to_float(env, vmaxIt->second);
        return std::make_unique<nsparse::SeismicSQSearchParameters>(
            vmin, vmax, cut, heapFactor);
    }

    return std::make_unique<nsparse::SeismicSearchParameters>(cut, heapFactor);
}

/**
 * Narrow query tokens to term_t, dropping any that cannot be represented.
 *
 * A plain std::vector<term_t>(tokens, tokens + n) wraps out-of-range ids, which
 * aliases distinct tokens onto one term and scores them as if they matched. A
 * token term_t cannot hold matches nothing, so dropping it preserves scores.
 *
 * Weights are filtered in lockstep so token/weight pairs stay aligned.
 *
 * This deliberately does NOT also bound tokens by the index dimension, even
 * though nsparse's search path indexes by term id unchecked (the seismic scratch
 * buffer is sized to the dimension and scattered into at `dense.data() + q_idx[i]
 * * element_size`; InvertedLists::operator[] is a bare vector subscript). The
 * dimension is not obtainable here: the JNI always builds an IDMapIndex, whose
 * constructor never initializes Index::dimension_ and which neither overrides
 * get_dimension() nor exposes its delegate. Filtering on that value drops every
 * token. Bounding by the dimension needs a fix in nsparse first.
 */
void retainSearchableTerms(const int32_t* tokens, const float* weights,
                           int numTokens,
                           std::vector<nsparse::term_t>& outTokens,
                           std::vector<float>& outWeights) {
    const auto termMax =
        static_cast<int32_t>(std::numeric_limits<nsparse::term_t>::max());
    outTokens.reserve(numTokens);
    outWeights.reserve(numTokens);
    for (int i = 0; i < numTokens; ++i) {
        if (tokens[i] >= 0 && tokens[i] <= termMax) {
            outTokens.push_back(static_cast<nsparse::term_t>(tokens[i]));
            outWeights.push_back(weights[i]);
        }
    }
}

// -1 is the sentinel buildSparseQueryResults() skips. The result arrays are
// zero-initialized, so they have to be marked explicitly when no search runs;
// otherwise every slot reads back as doc 0 with score 0.
void markNoResults(float* distances, int32_t* labels, int k) {
    std::fill_n(labels, k, -1);
    std::fill_n(distances, k, 0.0f);
}

}  // anonymous namespace

// ============================================================================
// NsparseWrapper method implementations
// ============================================================================

int64_t initIndex(int64_t numDocs, int dim,
                  const std::map<std::string, jobject>& parameters,
                  JNIEnv* env) {
    std::string description = buildDescription(parameters, env);
    nsparse::Index* index = nsparse::index_factory(dim, description.c_str());
    return reinterpret_cast<int64_t>(index);
}

void insertToIndex(int64_t indexAddress, const int32_t* ids, int numIds,
                   int64_t indicesAddress, int64_t tokensAddress,
                   int64_t valuesAddress, int threadCount) {
    auto* index = reinterpret_cast<nsparse::Index*>(indexAddress);
    // A segment with no documents for this field leaves the buffer unflushed and
    // its addresses at 0. Nothing to index, and dereferencing them would crash.
    // build() is skipped too: it dereferences state that add_with_ids populates.
    if (indicesAddress == 0 || tokensAddress == 0 || valuesAddress == 0) {
        return;
    }
    // insertToIndex consumes the off-heap buffers, so take ownership up front:
    // the range check below and add_with_ids/build() can all throw, and every one
    // of those paths must still free them. Java has already dropped its only
    // handle (the raw addresses), so anything not freed here is unreachable.
    std::unique_ptr<std::vector<int32_t>> indptr(
        reinterpret_cast<std::vector<int32_t>*>(indicesAddress));
    std::unique_ptr<std::vector<int32_t>> tokens(
        reinterpret_cast<std::vector<int32_t>*>(tokensAddress));
    std::unique_ptr<std::vector<float>> values(
        reinterpret_cast<std::vector<float>*>(valuesAddress));

    // nsparse uses uint16_t (term_t) for token indices. Narrowing silently wraps,
    // which would alias distinct tokens onto one term and score them together, so
    // refuse the segment instead: nsparse::Index::read_csr rejects the same range.
    const auto termLimit =
        static_cast<int32_t>(std::numeric_limits<nsparse::term_t>::max());
    for (int32_t token : *tokens) {
        if (token < 0 || token > termLimit) {
            throw std::invalid_argument(
                "sparse token id " + std::to_string(token) +
                " is outside the range [0, " + std::to_string(termLimit) +
                "] supported by the native engine");
        }
    }
    std::vector<nsparse::term_t> termTokens(tokens->begin(), tokens->end());

    omp_set_num_threads(threadCount);
    index->add_with_ids(static_cast<nsparse::idx_t>(numIds),
                        reinterpret_cast<const nsparse::idx_t*>(indptr->data()),
                        termTokens.data(), values->data(),
                        reinterpret_cast<const nsparse::idx_t*>(ids));

    index->build();
}

/**
 * Serialize the index to the given Java IndexOutputWrapper, then free the
 * index. Takes ownership of the index at indexAddress — the caller must not use
 * or free this address after writeIndex returns.
 */
void writeIndex(int64_t indexAddress, jobject output, JNIEnv* env) {
    std::unique_ptr<nsparse::Index> index(
        reinterpret_cast<nsparse::Index*>(indexAddress));
    auto jniWriter =
        std::make_unique<neural_search_jni::JniBufferedWriter>(env, output);
    neural_search_jni::NsparseStreamWriter streamWriter(std::move(jniWriter));
    nsparse::write_index(index.get(), &streamWriter);
    streamWriter.close();
}

int64_t loadIndex(const std::string& indexPath, int ioFlags) {
    // read_index takes char* (non-const in the nsparse API)
    std::string path = indexPath;
    nsparse::Index* index = nsparse::read_index(path.data(), ioFlags);
    return reinterpret_cast<int64_t>(index);
}

void queryIndex(int64_t indexAddress, const int32_t* tokens,
                const float* weights, int numTokens, int k,
                const std::map<std::string, jobject>& methodParameters,
                JNIEnv* env, float* distances, int32_t* labels) {
    auto* index = reinterpret_cast<nsparse::Index*>(indexAddress);

    std::vector<nsparse::term_t> termTokens;
    std::vector<float> termWeights;
    retainSearchableTerms(tokens, weights, numTokens, termTokens, termWeights);
    if (termTokens.empty()) {
        markNoResults(distances, labels, k);
        return;
    }

    // Build single-query CSR indptr: [0, numTokens]
    nsparse::idx_t indptr[2] = {0,
                                static_cast<nsparse::idx_t>(termTokens.size())};

    std::unique_ptr<nsparse::SearchParameters> params =
        methodParameters.empty() ? nullptr
                                 : buildSearchParameters(methodParameters, env);
    omp_set_num_threads(1);
    index->search(1, indptr, termTokens.data(), termWeights.data(), k, distances,
                  reinterpret_cast<nsparse::idx_t*>(labels), params.get());
}

void queryIndexWithFilter(
    int64_t indexAddress, const int32_t* tokens, const float* weights,
    int numTokens, int k,
    const std::map<std::string, jobject>& methodParameters,
    const int64_t* filterIds, int numFilterIds, int filterIdsType, JNIEnv* env,
    float* distances, int32_t* labels) {
    auto* index = reinterpret_cast<nsparse::Index*>(indexAddress);

    std::vector<nsparse::term_t> termTokens;
    std::vector<float> termWeights;
    retainSearchableTerms(tokens, weights, numTokens, termTokens, termWeights);
    if (termTokens.empty()) {
        markNoResults(distances, labels, k);
        return;
    }

    // Build single-query CSR indptr: [0, numTokens]
    nsparse::idx_t indptr[2] = {0,
                                static_cast<nsparse::idx_t>(termTokens.size())};

    // Convert int64_t filter IDs to idx_t (int32_t)
    std::vector<nsparse::idx_t> idxFilterIds(filterIds,
                                             filterIds + numFilterIds);

    // Build the appropriate IDSelector based on filterIdsType
    std::unique_ptr<nsparse::IDSelector> idSelector;
    if (filterIdsType == 0) {
        idSelector = std::make_unique<nsparse::SetIDSelector>(
            idxFilterIds.size(), idxFilterIds.data());
    } else {
        idSelector = std::make_unique<nsparse::ArrayIDSelector>(
            idxFilterIds.size(), idxFilterIds.data());
    }

    std::unique_ptr<nsparse::SearchParameters> params =
        methodParameters.empty()
            ? std::make_unique<nsparse::SeismicSearchParameters>()
            : buildSearchParameters(methodParameters, env);
    params->set_id_selector(idSelector.get());

    omp_set_num_threads(1);
    index->search(1, indptr, termTokens.data(), termWeights.data(), k, distances,
                  reinterpret_cast<nsparse::idx_t*>(labels), params.get());
}

void freeIndex(int64_t indexAddress) {
    auto* index = reinterpret_cast<nsparse::Index*>(indexAddress);
    delete index;
}

}  // namespace neural_search_jni::nsparse_wrapper
