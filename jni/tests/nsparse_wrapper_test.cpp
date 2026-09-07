/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

//
// Integration-style unit tests for the JNI wrapper layer:
//   * nsparse_wrapper.cpp   — index lifecycle (init/insert/write/load/query/free)
//   * nsparse_stream_writer — native buffer -> Java writeBytes streaming
//   * the JNI entry points   — argument marshalling and result construction
//
// All tests drive the REAL nsparse library through the wrapper API, using the
// fake JNIEnv to stand in for the JVM. Because they build, serialize, reload,
// and free real native indices, they also serve as leak tests when run under
// AddressSanitizer or valgrind (see the memory-leak notes in the CMake test
// target / TESTING docs).
//

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "common.h"
#include "fake_jni.h"
#include "jni_util.h"
#include "nsparse/index.h"
#include "nsparse/index_factory.h"
#include "nsparse/io/index_io.h"
#include "nsparse/io/io.h"
#include "nsparse_stream_writer.h"
#include "nsparse_wrapper.h"

using namespace neural_search_jni;
using neural_search_jni::test::FakeJniEnv;
namespace wrapper = neural_search_jni::nsparse_wrapper;

// ---------------------------------------------------------------------------
// Fixture: initializes the JNI class/method cache (needed by buildDescription /
// buildSearchParameters which call jobject_to_* helpers through the cache).
// ---------------------------------------------------------------------------
class NsparseWrapperTest : public ::testing::Test {
protected:
    FakeJniEnv fake;

    void SetUp() override { cachedRefs.init(fake.env()); }
    void TearDown() override { cachedRefs.release(fake.env()); }

    // Allocate an off-heap CSR triple and populate via transferVectors, exactly
    // as the JNI transferVectors entry point does. Returns the three addresses;
    // ownership passes to insertToIndex (which frees them).
    struct OffHeapAddrs {
        int64_t indices = 0;
        int64_t tokens = 0;
        int64_t values = 0;
    };

    OffHeapAddrs transfer(const std::vector<int32_t>& indptr,
                          const std::vector<int32_t>& tokens,
                          const std::vector<float>& weights) {
        int64_t addr[3] = {0, 0, 0};
        neural_search_jni::transferVectors(addr, indptr.data(), indptr.size(),
                                           tokens.data(), tokens.size(),
                                           weights.data(), weights.size());
        return {addr[0], addr[1], addr[2]};
    }
};

// ---------------------------------------------------------------------------
// initIndex / freeIndex
// ---------------------------------------------------------------------------

TEST_F(NsparseWrapperTest, InitIndexInvertedReturnsNonZeroAddress) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("inverted");

    int64_t addr = wrapper::initIndex(/*numDocs=*/3, /*dim=*/16, params, fake.env());
    ASSERT_NE(addr, 0);
    wrapper::freeIndex(addr);  // must not crash / leak
}

TEST_F(NsparseWrapperTest, InitIndexMissingIndexKeyThrows) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);  // no "index" key
    EXPECT_THROW(wrapper::initIndex(1, 8, params, fake.env()), std::invalid_argument);
}

TEST_F(NsparseWrapperTest, InitIndexSeismicSqBuildsFromParameters) {
    // Mirrors DefaultNativeIndexWriter.buildIndexParameters() for the seismic path.
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("seismic_sq");
    params["quantizer"] = fake.makeString("8bit");
    params["vmax"] = fake.makeNumber(3.0);
    params["vmin"] = fake.makeNumber(0.0);
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.4);

    int64_t addr = wrapper::initIndex(4, 32, params, fake.env());
    ASSERT_NE(addr, 0);
    wrapper::freeIndex(addr);
}

// The clustering_batch_size mapping parameter reaches nsparse as the pair
// inverted_list_batch_size + batch_file_output_path, which index_factory ignores
// unless both arrive. What it buys is build memory, so what is asserted here is
// that the description survives the trip: the batched build returns the same
// documents as the unbatched one and leaves no spill behind in the scratch
// directory it was given.
TEST_F(NsparseWrapperTest, SeismicSqBatchedBuildMatchesUnbatchedAndLeavesNoSpill) {
    const std::filesystem::path scratch =
        std::filesystem::path(::testing::TempDir()) / "nsparse_batched_build";
    std::filesystem::remove_all(scratch);
    ASSERT_TRUE(std::filesystem::create_directories(scratch));

    auto build = [&](bool batched) {
        std::map<std::string, jobject> params;
        params["idmap"] = fake.makeBool(true);
        params["index"] = fake.makeString("seismic_sq");
        params["quantizer"] = fake.makeString("8bit");
        params["vmin"] = fake.makeNumber(0.0);
        params["vmax"] = fake.makeNumber(3.0);
        params["lambda"] = fake.makeNumber(10);
        params["beta"] = fake.makeNumber(5.0);
        params["alpha"] = fake.makeNumber(0.4);
        if (batched) {
            params["inverted_list_batch_size"] = fake.makeNumber(4);
            params["batch_file_output_path"] = fake.makeString(scratch.string());
        }
        int64_t index = wrapper::initIndex(3, 16, params, fake.env());
        EXPECT_NE(index, 0);
        OffHeapAddrs off = transfer({0, 2, 4, 6}, {1, 2, 2, 3, 3, 4},
                                    {1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
        std::vector<int32_t> ids = {100, 200, 300};
        wrapper::insertToIndex(index, ids.data(), 3, off.indices, off.tokens,
                               off.values, /*threadCount=*/1);

        std::vector<int32_t> qTokens = {2};
        std::vector<float> qWeights = {1.0f};
        int k = 3;
        std::vector<float> distances(k, 0.0f);
        std::vector<int32_t> labels(k, -1);
        std::map<std::string, jobject> queryParams;
        wrapper::queryIndex(index, qTokens.data(), qWeights.data(), 1, k,
                            queryParams, fake.env(), distances.data(),
                            labels.data());
        wrapper::freeIndex(index);

        std::vector<int32_t> got;
        for (int i = 0; i < k; ++i) {
            if (labels[i] != -1) got.push_back(labels[i]);
        }
        return got;
    };

    std::vector<int32_t> unbatched = build(false);
    std::vector<int32_t> batched = build(true);
    ASSERT_FALSE(unbatched.empty());
    EXPECT_EQ(unbatched, batched);
    // The spill is unlinked once mapped, and the index that mapped it is freed.
    EXPECT_TRUE(std::filesystem::is_empty(scratch));
    std::filesystem::remove_all(scratch);
}

// That the pair reaches nsparse rather than being dropped on the way, which the
// comparison above cannot tell: a spill directory that does not exist is refused
// at build time, and only a description carrying both keys gets that far.
TEST_F(NsparseWrapperTest, BatchedBuildRejectsAMissingSpillDirectory) {
    const std::filesystem::path missing =
        std::filesystem::path(::testing::TempDir()) / "nsparse_no_such_dir";
    std::filesystem::remove_all(missing);

    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("seismic_sq");
    params["quantizer"] = fake.makeString("8bit");
    params["vmin"] = fake.makeNumber(0.0);
    params["vmax"] = fake.makeNumber(3.0);
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.4);
    params["inverted_list_batch_size"] = fake.makeNumber(4);
    params["batch_file_output_path"] = fake.makeString(missing.string());

    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);
    OffHeapAddrs off = transfer({0, 2, 4, 6}, {1, 2, 2, 3, 3, 4},
                                {1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {100, 200, 300};
    // insertToIndex frees the off-heap vectors on the throwing path too; the
    // index itself is the caller's to free.
    EXPECT_THROW(wrapper::insertToIndex(index, ids.data(), 3, off.indices,
                                        off.tokens, off.values,
                                        /*threadCount=*/1),
                 std::invalid_argument);
    wrapper::freeIndex(index);
}

TEST_F(NsparseWrapperTest, FreeIndexOnZeroAddressIsNoop) {
    // delete nullptr is well-defined; must not crash.
    wrapper::freeIndex(0);
}

// ---------------------------------------------------------------------------
// Full lifecycle: insert -> query (in-memory, no serialization)
// ---------------------------------------------------------------------------

TEST_F(NsparseWrapperTest, InsertThenQueryReturnsExternalIds) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("inverted");
    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);

    // 3 docs, CSR indptr [0,2,4,6]; tokens/weights per doc.
    //   doc0: tokens {1,2}
    //   doc1: tokens {2,3}
    //   doc2: tokens {3,4}
    OffHeapAddrs off = transfer(/*indptr=*/{0, 2, 4, 6},
                                /*tokens=*/{1, 2, 2, 3, 3, 4},
                                /*weights=*/{1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
    // External document ids.
    std::vector<int32_t> ids = {100, 200, 300};

    // insertToIndex consumes (and frees) the off-heap vectors.
    wrapper::insertToIndex(index, ids.data(), static_cast<int>(ids.size()),
                           off.indices, off.tokens, off.values, /*threadCount=*/1);

    // Query with token {2} — should match doc0 and doc1 (external ids 100,200).
    std::vector<int32_t> qTokens = {2};
    std::vector<float> qWeights = {1.0f};
    int k = 3;
    std::vector<float> distances(k, 0.0f);
    std::vector<int32_t> labels(k, -1);
    std::map<std::string, jobject> queryParams;  // empty -> null SearchParameters

    wrapper::queryIndex(index, qTokens.data(), qWeights.data(),
                        static_cast<int>(qTokens.size()), k, queryParams,
                        fake.env(), distances.data(), labels.data());

    // Collect returned external ids (padding is INVALID_IDX == -1).
    std::vector<int32_t> got;
    for (int i = 0; i < k; ++i) {
        if (labels[i] != -1) got.push_back(labels[i]);
    }
    ASSERT_FALSE(got.empty());
    for (int32_t id : got) {
        EXPECT_TRUE(id == 100 || id == 200 || id == 300)
            << "unexpected external id " << id;
    }

    wrapper::freeIndex(index);
}

// Tokens beyond term_t used to be narrowed by the vector<term_t> range
// constructor, which wraps: 65538 became 2 and scored against a real term.
TEST_F(NsparseWrapperTest, QueryDropsTokensTooLargeForTermType) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("inverted");
    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);

    // doc0 holds token 2; nothing holds a token that 65538 could wrap onto.
    OffHeapAddrs off = transfer({0, 1}, {2}, {1.0f});
    std::vector<int32_t> ids = {100};
    wrapper::insertToIndex(index, ids.data(), 1, off.indices, off.tokens,
                           off.values, /*threadCount=*/1);

    // 65538 wraps to 2 under uint16_t truncation, and -1 is never a valid term.
    std::vector<int32_t> qTokens = {65538, -1};
    std::vector<float> qWeights = {1.0f, 1.0f};
    int k = 3;
    std::vector<float> distances(k, 0.0f);
    std::vector<int32_t> labels(k, 0);
    std::map<std::string, jobject> queryParams;

    wrapper::queryIndex(index, qTokens.data(), qWeights.data(), 2, k,
                        queryParams, fake.env(), distances.data(),
                        labels.data());

    for (int i = 0; i < k; ++i) {
        EXPECT_EQ(labels[i], -1)
            << "wrapped token matched doc " << labels[i] << " at slot " << i;
    }

    wrapper::freeIndex(index);
}

// A segment with no documents for the field leaves the buffer unflushed, so all
// three addresses are 0. Dereferencing them crashed the merge thread.
TEST_F(NsparseWrapperTest, InsertWithZeroAddressesIsNoop) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("inverted");
    int64_t index = wrapper::initIndex(0, 16, params, fake.env());
    ASSERT_NE(index, 0);

    wrapper::insertToIndex(index, nullptr, 0, 0, 0, 0, /*threadCount=*/1);

    wrapper::freeIndex(index);
}

// Ingesting a token term_t cannot hold must fail loudly rather than alias it
// onto another term, matching nsparse::Index::read_csr's own range check.
TEST_F(NsparseWrapperTest, InsertRejectsTokenBeyondTermType) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("inverted");
    int64_t index = wrapper::initIndex(1, 70000, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 1}, {70000}, {1.0f});
    std::vector<int32_t> ids = {100};
    EXPECT_THROW(
        wrapper::insertToIndex(index, ids.data(), 1, off.indices, off.tokens,
                               off.values, /*threadCount=*/1),
        std::invalid_argument);

    wrapper::freeIndex(index);
}

TEST_F(NsparseWrapperTest, QueryWithFilterRestrictsResults) {
    // Filtering (the IDSelector) is honored by the seismic index family; the
    // plain inverted index ignores it. Use seismic here so the filter has effect.
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("seismic");
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);
    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 1, 2, 3}, {5, 5, 5}, {1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {10, 20, 30};
    wrapper::insertToIndex(index, ids.data(), 3, off.indices, off.tokens,
                           off.values, 1);

    // Filter to only external id 20 (SetIDSelector, filterIdsType == 0).
    std::vector<int64_t> filter = {20};
    std::vector<int32_t> qTokens = {5};
    std::vector<float> qWeights = {1.0f};
    int k = 3;
    std::vector<float> distances(k, 0.0f);
    std::vector<int32_t> labels(k, -1);
    std::map<std::string, jobject> queryParams;

    wrapper::queryIndexWithFilter(index, qTokens.data(), qWeights.data(), 1, k,
                                  queryParams, filter.data(), 1,
                                  /*filterIdsType=*/0, fake.env(),
                                  distances.data(), labels.data());

    for (int i = 0; i < k; ++i) {
        if (labels[i] != -1) {
            EXPECT_EQ(labels[i], 20) << "filter should restrict to id 20";
        }
    }
    wrapper::freeIndex(index);
}

// filterIdsType != 0 selects ArrayIDSelector instead of SetIDSelector. Both must
// restrict to the same documents; only the lookup structure differs.
TEST_F(NsparseWrapperTest, QueryWithFilterSupportsBothIdSelectorTypes) {
    auto runFilteredQuery = [&](int filterIdsType, std::vector<int32_t>& labels) {
        std::map<std::string, jobject> params;
        params["idmap"] = fake.makeBool(true);
        params["index"] = fake.makeString("seismic");
        params["lambda"] = fake.makeNumber(10);
        params["beta"] = fake.makeNumber(5.0);
        params["alpha"] = fake.makeNumber(0.5);
        int64_t index = wrapper::initIndex(3, 16, params, fake.env());
        ASSERT_NE(index, 0);

        OffHeapAddrs off = transfer({0, 1, 2, 3}, {5, 5, 5}, {1.0f, 1.0f, 1.0f});
        std::vector<int32_t> ids = {10, 20, 30};
        wrapper::insertToIndex(index, ids.data(), 3, off.indices, off.tokens,
                               off.values, 1);

        std::vector<int64_t> filter = {20};
        std::vector<int32_t> qTokens = {5};
        std::vector<float> qWeights = {1.0f};
        const int k = 3;
        std::vector<float> distances(k, 0.0f);
        labels.assign(k, -1);
        std::map<std::string, jobject> queryParams;

        wrapper::queryIndexWithFilter(index, qTokens.data(), qWeights.data(), 1, k,
                                      queryParams, filter.data(), 1, filterIdsType,
                                      fake.env(), distances.data(), labels.data());
        wrapper::freeIndex(index);
    };

    std::vector<int32_t> setLabels;
    std::vector<int32_t> arrayLabels;
    runFilteredQuery(/*SetIDSelector=*/0, setLabels);
    runFilteredQuery(/*ArrayIDSelector=*/1, arrayLabels);

    for (int32_t id : arrayLabels) {
        EXPECT_TRUE(id == -1 || id == 20)
            << "ArrayIDSelector let through unfiltered id " << id;
    }
    EXPECT_EQ(arrayLabels, setLabels)
        << "the two selector types disagreed on the same filter";
}

// ---------------------------------------------------------------------------
// Serialization: writeIndex path.
//
// writeIndex streams the serialized index into the fake IndexOutputWrapper's
// sink. We verify the captured bytes are a VALID serialized index by handing
// them back to nsparse::read_index through an in-memory reader whose close() is
// a no-op.
//
// NOTE: we deliberately do NOT round-trip through wrapper::loadIndex here for
// the idmap-wrapped index — see the DISABLED regression test below and the
// review notes. loadIndex uses nsparse FileIOReader whose close() truly closes
// the FILE*, and a nested (idmap) read_index closes the shared stream out from
// under the outer read, crashing. The bytes themselves are correct; the defect
// is in nsparse's FileIO close semantics for nested serialization.
// ---------------------------------------------------------------------------

namespace {
// In-memory IOReader whose close() is intentionally a no-op, so nested
// read_index calls (as done by IDMapIndex) don't tear down the shared stream.
class MemReader : public nsparse::IOReader {
public:
    explicit MemReader(std::vector<char> data) : data_(std::move(data)) {}
    size_t read(void* ptr, size_t size, size_t nitems) override {
        size_t want = size * nitems;
        size_t avail = data_.size() - pos_;
        size_t take = std::min(want, avail);
        if (take > 0) {
            std::memcpy(ptr, data_.data() + pos_, take);
            pos_ += take;
        }
        return take / size;
    }
    size_t pos() const override { return pos_; }
    void close() override {}

private:
    std::vector<char> data_;
    size_t pos_ = 0;
};
}  // namespace

TEST_F(NsparseWrapperTest, WriteIndexProducesDeserializableBytes) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("inverted");
    int64_t index = wrapper::initIndex(2, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 2, 4}, {1, 2, 2, 3}, {1.0f, 1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {7, 8};
    wrapper::insertToIndex(index, ids.data(), 2, off.indices, off.tokens,
                           off.values, 1);

    // writeIndex streams into the sink and frees the index (takes ownership).
    std::vector<char> serialized;
    jobject output = fake.makeOutput(&serialized);
    wrapper::writeIndex(index, output, fake.env());
    ASSERT_FALSE(serialized.empty());

    // The captured bytes must deserialize into a valid index (idmap header
    // 'IDMP' + inverted delegate 'INVT'). Uses a no-op-close reader.
    MemReader reader(serialized);
    nsparse::Index* loaded = nsparse::read_index(&reader);
    ASSERT_NE(loaded, nullptr);

    // Query the reloaded index; token {2} matches both docs (external ids 7,8).
    int k = 2;
    std::vector<float> distances(k, 0.0f);
    std::vector<nsparse::idx_t> labels(k, -1);
    nsparse::idx_t qIndptr[2] = {0, 1};
    std::vector<nsparse::term_t> qTokens = {2};
    std::vector<float> qWeights = {1.0f};
    loaded->search(1, qIndptr, qTokens.data(), qWeights.data(), k,
                   distances.data(), labels.data(), nullptr);
    std::vector<int32_t> got;
    for (int i = 0; i < k; ++i) {
        if (labels[i] != -1) got.push_back(labels[i]);
    }
    ASSERT_FALSE(got.empty());
    for (int32_t id : got) {
        EXPECT_TRUE(id == 7 || id == 8) << "unexpected external id " << id;
    }
    delete loaded;
}

// loadIndex on a single-level (non-idmap) index reads cleanly through nsparse
// FileIOReader — there is no nested read_index to close the shared stream. This
// exercises the wrapper::loadIndex + queryIndex + freeIndex path end-to-end.
TEST_F(NsparseWrapperTest, LoadIndexAndQuerySingleLevel) {
    // Build a single-level inverted index directly (add(), not add_with_ids)
    // and persist it via nsparse's file writer.
    nsparse::Index* idx = nsparse::index_factory(16, "inverted");
    nsparse::idx_t indptr[3] = {0, 2, 4};
    std::vector<nsparse::term_t> tok = {1, 2, 2, 3};
    std::vector<float> w = {1.0f, 1.0f, 1.0f, 1.0f};
    idx->add(2, indptr, tok.data(), w.data());
    idx->build();

    std::string path = std::string(::testing::TempDir()) + "/nsparse_single.idx";
    std::vector<char> pathBuf(path.begin(), path.end());
    pathBuf.push_back('\0');
    nsparse::write_index(idx, pathBuf.data());
    delete idx;

    int64_t loaded = wrapper::loadIndex(path);
    ASSERT_NE(loaded, 0);

    std::vector<int32_t> qTokens = {2};
    std::vector<float> qWeights = {1.0f};
    int k = 2;
    std::vector<float> distances(k, 0.0f);
    std::vector<int32_t> labels(k, -1);
    std::map<std::string, jobject> queryParams;
    wrapper::queryIndex(loaded, qTokens.data(), qWeights.data(), 1, k,
                        queryParams, fake.env(), distances.data(), labels.data());

    // Internal ids 0 and 1 both contain token 2.
    bool anyHit = false;
    for (int i = 0; i < k; ++i) {
        if (labels[i] != -1) anyHit = true;
    }
    EXPECT_TRUE(anyHit);

    wrapper::freeIndex(loaded);
    std::remove(path.c_str());
}

namespace {
// Writes an index through the production stream writer to a real file, which is
// what wrapper::loadIndex consumes.
std::string writeIndexToFile(FakeJniEnv& fake, int64_t index,
                             const std::string& name) {
    std::vector<char> serialized;
    jobject output = fake.makeOutput(&serialized);
    wrapper::writeIndex(index, output, fake.env());

    std::string path = std::string(::testing::TempDir()) + "/" + name;
    FILE* f = std::fopen(path.c_str(), "wb");
    if (f == nullptr) return {};
    std::fwrite(serialized.data(), 1, serialized.size(), f);
    std::fclose(f);
    return path;
}
}  // namespace

// Was disabled as a known nsparse bug: FileIOReader::close() fclose()d the FILE*
// while IDMapIndex::read_index ran a NESTED read_index on the same reader, so the
// outer read fread from a closed stream. The submodule bump fixed it by threading
// keep_open through detail::read_index, and idmap is on for every native segment.
TEST_F(NsparseWrapperTest, LoadIdmapIndexFromDiskRoundTrip) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("inverted");
    int64_t index = wrapper::initIndex(2, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 2, 4}, {1, 2, 2, 3}, {1.0f, 1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {7, 8};
    wrapper::insertToIndex(index, ids.data(), 2, off.indices, off.tokens,
                           off.values, 1);

    std::string path = writeIndexToFile(fake, index, "nsparse_idmap.idx");
    ASSERT_FALSE(path.empty());

    int64_t loaded = wrapper::loadIndex(path);
    ASSERT_NE(loaded, 0);
    wrapper::freeIndex(loaded);
    std::remove(path.c_str());
}

// The production shape: idmap wrapping a plain seismic index, loaded with
// kUseMmap. Mapping is only reachable because IDMapIndex::read_index forwards
// io_flags to its SEIS delegate, and it must return the same external ids and
// scores as the copying read of the same file.
TEST_F(NsparseWrapperTest, MmapLoadMatchesCopyingLoadForIdmapSeismic) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("seismic");
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);
    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 2, 4, 6}, {1, 2, 2, 3, 3, 4},
                                {1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {100, 200, 300};
    wrapper::insertToIndex(index, ids.data(), 3, off.indices, off.tokens,
                           off.values, 1);

    std::string path = writeIndexToFile(fake, index, "nsparse_mmap_seismic.idx");
    ASSERT_FALSE(path.empty());

    std::vector<int32_t> qTokens = {2};
    std::vector<float> qWeights = {1.0f};
    const int k = 3;
    std::map<std::string, jobject> queryParams;

    auto query = [&](int64_t handle, std::vector<int32_t>& labels,
                     std::vector<float>& distances) {
        labels.assign(k, 0);
        distances.assign(k, 0.0f);
        wrapper::queryIndex(handle, qTokens.data(), qWeights.data(), 1, k,
                            queryParams, fake.env(), distances.data(),
                            labels.data());
    };

    int64_t copied = wrapper::loadIndex(path, 0);
    ASSERT_NE(copied, 0);
    std::vector<int32_t> copiedLabels;
    std::vector<float> copiedScores;
    query(copied, copiedLabels, copiedScores);
    wrapper::freeIndex(copied);

    int64_t mapped = wrapper::loadIndex(path, nsparse::IndexIoFlag::kUseMmap);
    ASSERT_NE(mapped, 0);
    std::vector<int32_t> mappedLabels;
    std::vector<float> mappedScores;
    query(mapped, mappedLabels, mappedScores);
    wrapper::freeIndex(mapped);

    EXPECT_EQ(mappedLabels, copiedLabels);
    EXPECT_EQ(mappedScores, copiedScores);
    // External ids survive the mapped read rather than leaking internal ordinals.
    for (int32_t id : mappedLabels) {
        EXPECT_TRUE(id == -1 || id == 100 || id == 200 || id == 300)
            << "unexpected id " << id;
    }
    std::remove(path.c_str());
}

// ---------------------------------------------------------------------------
// DiskSeismicIndex (DSEI).
//
// The disk variant keeps cluster summaries in RAM and borrows the per-document
// forward vectors from a file mapping, so unlike every other index type it has
// NO copying read path: read_index() throws and only the mmap load works. These
// tests pin that the JNI paths (write through the Java IndexOutput, load with
// kUseMmap, query) work for it, since a regression would only surface at runtime.
// ---------------------------------------------------------------------------

TEST_F(NsparseWrapperTest, InitIndexDiskSeismicBuildsFromParameters) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("disk_seismic");
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);

    int64_t addr = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(addr, 0);
    wrapper::freeIndex(addr);
}

// The full production shape for the disk engine: idmap over disk_seismic, written
// through the Java IndexOutput and mapped back in. Also covers the writer's pos()
// contract, which DSEI leans on harder than the other types --
// InlineForwardIndex::serialize pads against pos() and then asserts that the body
// length it wrote matches the length prefix, throwing if pos() ever drifts.
TEST_F(NsparseWrapperTest, DiskSeismicRoundTripsThroughMmapLoad) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("disk_seismic");
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);
    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 2, 4, 6}, {1, 2, 2, 3, 3, 4},
                                {1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {100, 200, 300};
    wrapper::insertToIndex(index, ids.data(), 3, off.indices, off.tokens,
                           off.values, 1);

    std::string path = writeIndexToFile(fake, index, "nsparse_disk_seismic.idx");
    ASSERT_FALSE(path.empty());

    int64_t mapped = wrapper::loadIndex(path, nsparse::IndexIoFlag::kUseMmap);
    ASSERT_NE(mapped, 0);

    std::vector<int32_t> qTokens = {2};
    std::vector<float> qWeights = {1.0f};
    const int k = 3;
    std::vector<float> distances(k, 0.0f);
    std::vector<int32_t> labels(k, 0);
    std::map<std::string, jobject> queryParams;
    wrapper::queryIndex(mapped, qTokens.data(), qWeights.data(), 1, k,
                        queryParams, fake.env(), distances.data(),
                        labels.data());

    bool anyHit = false;
    for (int i = 0; i < k; ++i) {
        if (labels[i] != -1) {
            anyHit = true;
            EXPECT_TRUE(labels[i] == 100 || labels[i] == 200 || labels[i] == 300)
                << "unexpected external id " << labels[i];
        }
    }
    EXPECT_TRUE(anyHit) << "token 2 matches two docs; the mapped read returned none";

    wrapper::freeIndex(mapped);
    std::remove(path.c_str());
}

// k_prime, DSEI's block budget, lives only on DiskSeismicSearchParameters --
// DiskSeismicIndex::search() dynamic_casts for it and falls back to
// kDefaultBlockBudget for any other SearchParameters subtype. So a query map
// carrying k_prime has to produce the disk subtype, or the engine's primary
// recall/latency knob is silently unreachable from Java.
//
// A non-positive budget is rejected by the index, which is what makes this
// observable at all: k_prime=0 must surface as an error rather than be ignored.
TEST_F(NsparseWrapperTest, DiskSeismicReceivesKPrimeFromJavaParameters) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("disk_seismic");
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);
    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 1, 2, 3}, {5, 5, 5}, {1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {10, 20, 30};
    wrapper::insertToIndex(index, ids.data(), 3, off.indices, off.tokens,
                           off.values, 1);
    std::string path = writeIndexToFile(fake, index, "nsparse_disk_kprime.idx");
    ASSERT_FALSE(path.empty());
    int64_t mapped = wrapper::loadIndex(path, nsparse::IndexIoFlag::kUseMmap);
    ASSERT_NE(mapped, 0);

    std::vector<int32_t> qTokens = {5};
    std::vector<float> qWeights = {1.0f};
    const int k = 3;
    std::vector<float> distances(k, 0.0f);
    std::vector<int32_t> labels(k, -1);

    // A valid budget searches normally.
    std::map<std::string, jobject> okParams;
    okParams["cut"] = fake.makeNumber(4);
    okParams["k_prime"] = fake.makeNumber(8);
    wrapper::queryIndex(mapped, qTokens.data(), qWeights.data(), 1, k, okParams,
                        fake.env(), distances.data(), labels.data());
    bool anyHit = false;
    for (int i = 0; i < k; ++i) {
        if (labels[i] != -1) anyHit = true;
    }
    EXPECT_TRUE(anyHit);

    // A zero budget only reaches the index if the disk subtype was built; if the
    // JNI had emitted a plain SeismicSearchParameters this would quietly succeed
    // on the default budget instead.
    std::map<std::string, jobject> badParams;
    badParams["k_prime"] = fake.makeNumber(0);
    EXPECT_THROW(
        wrapper::queryIndex(mapped, qTokens.data(), qWeights.data(), 1, k,
                            badParams, fake.env(), distances.data(),
                            labels.data()),
        std::invalid_argument)
        << "k_prime never reached DiskSeismicIndex; the JNI built the wrong "
           "SearchParameters subtype";

    wrapper::freeIndex(mapped);
    std::remove(path.c_str());
}

TEST_F(NsparseWrapperTest, InitIndexDiskSeismicSqBuildsFromParameters) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("disk_seismic_sq");
    params["quantizer"] = fake.makeString("8bit");
    params["vmin"] = fake.makeNumber(0.0);
    params["vmax"] = fake.makeNumber(3.0);
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);

    int64_t addr = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(addr, 0);
    wrapper::freeIndex(addr);
}

// The production shape the plugin writes for forward_index=per_block: idmap over
// disk_seismic_sq, written through the Java IndexOutput and mapped back in. The
// query carries the search-side range plus k_prime, which is what makes
// buildSearchParameters emit DiskSeismicSQSearchParameters.
TEST_F(NsparseWrapperTest, DiskSeismicSqRoundTripsThroughMmapLoad) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("disk_seismic_sq");
    params["quantizer"] = fake.makeString("8bit");
    params["vmin"] = fake.makeNumber(0.0);
    params["vmax"] = fake.makeNumber(3.0);
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);
    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 2, 4, 6}, {1, 2, 2, 3, 3, 4},
                                {1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {100, 200, 300};
    wrapper::insertToIndex(index, ids.data(), 3, off.indices, off.tokens,
                           off.values, 1);

    std::string path = writeIndexToFile(fake, index, "nsparse_disk_seismic_sq.idx");
    ASSERT_FALSE(path.empty());

    int64_t mapped = wrapper::loadIndex(path, nsparse::IndexIoFlag::kUseMmap);
    ASSERT_NE(mapped, 0);

    std::vector<int32_t> qTokens = {2};
    std::vector<float> qWeights = {1.0f};
    const int k = 3;
    std::vector<float> distances(k, 0.0f);
    std::vector<int32_t> labels(k, -1);
    std::map<std::string, jobject> queryParams;
    queryParams["cut"] = fake.makeNumber(4);
    queryParams["vmin"] = fake.makeNumber(0.0);
    queryParams["vmax"] = fake.makeNumber(16.0);
    queryParams["k_prime"] = fake.makeNumber(50);
    wrapper::queryIndex(mapped, qTokens.data(), qWeights.data(), 1, k,
                        queryParams, fake.env(), distances.data(),
                        labels.data());

    bool anyHit = false;
    for (int i = 0; i < k; ++i) {
        if (labels[i] != -1) {
            anyHit = true;
            EXPECT_TRUE(labels[i] == 100 || labels[i] == 200 || labels[i] == 300)
                << "unexpected external id " << labels[i];
        }
    }
    EXPECT_TRUE(anyHit) << "token 2 matches two docs; the mapped read returned none";

    wrapper::freeIndex(mapped);
    std::remove(path.c_str());
}

// Only DiskSeismicSQSearchParameters carries a query range into disk_seismic_sq;
// with anything else the index reuses its build-time range, quantizing the query
// at the ingest ceiling and leaving the decoded scores unscaled. The subtype is
// selected by k_prime + vmin/vmax together, so this pins that both keys are read:
// two different query ranges over one index have to produce different scores.
TEST_F(NsparseWrapperTest, DiskSeismicSqAppliesQueryRangeAlongsideKPrime) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("disk_seismic_sq");
    params["quantizer"] = fake.makeString("8bit");
    params["vmin"] = fake.makeNumber(0.0);
    params["vmax"] = fake.makeNumber(3.0);
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);
    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 1, 2, 3}, {5, 5, 5}, {1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {10, 20, 30};
    wrapper::insertToIndex(index, ids.data(), 3, off.indices, off.tokens,
                           off.values, 1);
    std::string path = writeIndexToFile(fake, index, "nsparse_disk_sq_range.idx");
    ASSERT_FALSE(path.empty());
    int64_t mapped = wrapper::loadIndex(path, nsparse::IndexIoFlag::kUseMmap);
    ASSERT_NE(mapped, 0);

    std::vector<int32_t> qTokens = {5};
    std::vector<float> qWeights = {1.0f};
    const int k = 3;

    auto scoreWithRange = [&](double vmax) {
        std::vector<float> distances(k, 0.0f);
        std::vector<int32_t> labels(k, -1);
        std::map<std::string, jobject> queryParams;
        queryParams["cut"] = fake.makeNumber(4);
        queryParams["k_prime"] = fake.makeNumber(8);
        queryParams["vmin"] = fake.makeNumber(0.0);
        queryParams["vmax"] = fake.makeNumber(vmax);
        wrapper::queryIndex(mapped, qTokens.data(), qWeights.data(), 1, k,
                            queryParams, fake.env(), distances.data(),
                            labels.data());
        EXPECT_NE(labels[0], -1);
        return distances[0];
    };

    // The index was built over [0, 3]; a query encoded over [0, 16] decodes to a
    // different score. Equal scores mean the range never reached the index.
    EXPECT_NE(scoreWithRange(3.0), scoreWithRange(16.0));

    // A zero budget only reaches the index if a DiskSeismic subtype was built, so
    // this also pins that adding the range did not downgrade it to the plain
    // quantized subtype.
    std::vector<float> distances(k, 0.0f);
    std::vector<int32_t> labels(k, -1);
    std::map<std::string, jobject> badParams;
    badParams["k_prime"] = fake.makeNumber(0);
    badParams["vmin"] = fake.makeNumber(0.0);
    badParams["vmax"] = fake.makeNumber(16.0);
    EXPECT_THROW(
        wrapper::queryIndex(mapped, qTokens.data(), qWeights.data(), 1, k,
                            badParams, fake.env(), distances.data(),
                            labels.data()),
        std::invalid_argument)
        << "k_prime never reached the index; the JNI built the wrong "
           "SearchParameters subtype";

    wrapper::freeIndex(mapped);
    std::remove(path.c_str());
}

// The other index types must keep working when k_prime is absent, and must not be
// disturbed if it is present (DiskSeismicSearchParameters derives from
// SeismicSearchParameters, so a SeismicIndex still finds cut on it).
TEST_F(NsparseWrapperTest, SeismicToleratesDiskSearchParameters) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("seismic");
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);
    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 1, 2, 3}, {5, 5, 5}, {1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {10, 20, 30};
    wrapper::insertToIndex(index, ids.data(), 3, off.indices, off.tokens,
                           off.values, 1);

    std::vector<int32_t> qTokens = {5};
    std::vector<float> qWeights = {1.0f};
    const int k = 3;
    std::vector<float> distances(k, 0.0f);
    std::vector<int32_t> labels(k, -1);

    // k_prime makes buildSearchParameters emit the disk subtype; a plain
    // SeismicIndex must still read cut off it and return the same documents.
    std::map<std::string, jobject> queryParams;
    queryParams["cut"] = fake.makeNumber(4);
    queryParams["k_prime"] = fake.makeNumber(8);
    wrapper::queryIndex(index, qTokens.data(), qWeights.data(), 1, k, queryParams,
                        fake.env(), distances.data(), labels.data());

    bool anyHit = false;
    for (int i = 0; i < k; ++i) {
        if (labels[i] != -1) {
            anyHit = true;
            EXPECT_TRUE(labels[i] == 10 || labels[i] == 20 || labels[i] == 30);
        }
    }
    EXPECT_TRUE(anyHit);

    wrapper::freeIndex(index);
}

// The copying read is unsupported for DSEI, so a load that does not ask for mmap
// must fail loudly rather than return a half-built index. loadIndex() always
// passes kUseMmap today; this pins the behaviour the JNI depends on.
TEST_F(NsparseWrapperTest, DiskSeismicRejectsNonMmapLoad) {
    std::map<std::string, jobject> params;
    // idmap is required, not decoration: DiskSeismicIndex implements add() only,
    // so add_with_ids has to come from the IDMapIndex wrapper.
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("disk_seismic");
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);
    int64_t index = wrapper::initIndex(2, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 2, 4}, {1, 2, 2, 3}, {1.0f, 1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {7, 8};
    wrapper::insertToIndex(index, ids.data(), 2, off.indices, off.tokens,
                           off.values, 1);
    std::string path = writeIndexToFile(fake, index, "nsparse_disk_nommap.idx");
    ASSERT_FALSE(path.empty());

    EXPECT_THROW(wrapper::loadIndex(path, /*ioFlags=*/0), std::runtime_error);
    std::remove(path.c_str());
}

// ---------------------------------------------------------------------------
// NsparseStreamWriter / JniBufferedWriter buffering behaviour.
// ---------------------------------------------------------------------------

TEST_F(NsparseWrapperTest, StreamWriterFlushesExactBytes) {
    std::vector<char> sink;
    jobject output = fake.makeOutput(&sink);
    auto jniWriter = std::make_unique<JniBufferedWriter>(fake.env(), output);
    NsparseStreamWriter writer(std::move(jniWriter));

    std::vector<char> payload = {'a', 'b', 'c', 'd'};
    writer.write(payload.data(), sizeof(char), payload.size());
    writer.close();  // flush

    EXPECT_EQ(sink, payload);
}

TEST_F(NsparseWrapperTest, StreamWriterHandlesLargePayloadAcrossBufferBoundary) {
    std::vector<char> sink;
    jobject output = fake.makeOutput(&sink);
    auto jniWriter = std::make_unique<JniBufferedWriter>(fake.env(), output);
    NsparseStreamWriter writer(std::move(jniWriter));

    // 200 KB > the internal 64 KB buffer -> forces multiple flushes.
    const size_t n = 200 * 1024;
    std::vector<char> payload(n);
    for (size_t i = 0; i < n; ++i) payload[i] = static_cast<char>(i & 0xFF);

    writer.write(payload.data(), sizeof(char), payload.size());
    writer.close();

    ASSERT_EQ(sink.size(), n);
    EXPECT_EQ(sink, payload);
}

// pos() must report the ABSOLUTE stream offset, not the offset into the flush
// buffer. nsparse pads structures against this value, so a buffer-relative
// answer silently misaligns everything written after the first 64 KB flush.
TEST_F(NsparseWrapperTest, StreamWriterPosTracksAbsoluteOffsetAcrossFlushes) {
    std::vector<char> sink;
    jobject output = fake.makeOutput(&sink);
    auto jniWriter = std::make_unique<JniBufferedWriter>(fake.env(), output);
    NsparseStreamWriter writer(std::move(jniWriter));

    EXPECT_EQ(writer.pos(), 0u);

    std::vector<char> chunk(1000, 'x');
    writer.write(chunk.data(), sizeof(char), chunk.size());
    EXPECT_EQ(writer.pos(), 1000u);

    // Cross the 64 KB buffer boundary; pos() must keep counting, not rewind.
    std::vector<char> big(100 * 1024, 'y');
    writer.write(big.data(), sizeof(char), big.size());
    EXPECT_EQ(writer.pos(), 1000u + 100u * 1024u);

    writer.write(chunk.data(), sizeof(char), chunk.size());
    EXPECT_EQ(writer.pos(), 2000u + 100u * 1024u);

    writer.close();
    // Everything counted was actually handed to Java.
    EXPECT_EQ(sink.size(), writer.pos());
}

// pos() starts at the IndexOutput's file pointer, not at zero.
//
// nsparse pads every serialized array up to its element alignment using pos(),
// and the mmap reader recomputes that padding from the array's offset in the
// FILE (MmapCursor is seeded with the payload's absolute offset, and read_array
// throws "array is misaligned for its element type" if the arithmetic disagrees).
// A writer that counted only its own bytes would therefore agree with the reader
// only while the payload happened to start at offset 0 -- true of the codec
// today, but nothing enforced it, and a codec header written before the native
// payload would have silently broken every mapped load.
TEST_F(NsparseWrapperTest, StreamWriterPosStartsAtIndexOutputFilePointer) {
    std::vector<char> sink;
    // Simulate an IndexOutput that already holds a 37-byte header: deliberately
    // not a multiple of any element alignment, so wrong arithmetic shows up.
    const int64_t headerBytes = 37;
    jobject output = fake.makeOutput(&sink, headerBytes);
    auto jniWriter = std::make_unique<JniBufferedWriter>(fake.env(), output);
    EXPECT_EQ(jniWriter->startOffset(), static_cast<size_t>(headerBytes));

    NsparseStreamWriter writer(std::move(jniWriter));
    EXPECT_EQ(writer.pos(), static_cast<size_t>(headerBytes))
        << "pos() must report the file offset, not the count of bytes written";

    std::vector<char> payload(11, 'z');
    writer.write(payload.data(), sizeof(char), payload.size());
    EXPECT_EQ(writer.pos(), static_cast<size_t>(headerBytes) + payload.size());
    writer.close();

    // Only our own bytes reach the sink; the header is the caller's business.
    EXPECT_EQ(sink.size(), payload.size());
}

// The common case stays exactly as before: a fresh output reports offset 0.
TEST_F(NsparseWrapperTest, StreamWriterPosStartsAtZeroForAFreshOutput) {
    std::vector<char> sink;
    jobject output = fake.makeOutput(&sink);
    auto jniWriter = std::make_unique<JniBufferedWriter>(fake.env(), output);
    EXPECT_EQ(jniWriter->startOffset(), 0u);
    NsparseStreamWriter writer(std::move(jniWriter));
    EXPECT_EQ(writer.pos(), 0u);
}

// getFilePointer is resolved reflectively like writeBytes, so a rename on the
// Java side has to fail at construction rather than corrupt the padding.
TEST_F(NsparseWrapperTest, JniBufferedWriterRejectsMissingGetFilePointerMethod) {
    std::vector<char> sink;
    jobject output = fake.makeOutput(&sink);
    fake.missingMethodName = "getFilePointer";

    EXPECT_THROW(JniBufferedWriter(fake.env(), output), std::runtime_error);
    EXPECT_FALSE(fake.hasPendingException());
}

// Element size is multiplied by item count: nsparse writes typed arrays, so a
// write(ptr, sizeof(int32_t), n) must advance by 4n bytes.
TEST_F(NsparseWrapperTest, StreamWriterAccountsForElementSize) {
    std::vector<char> sink;
    jobject output = fake.makeOutput(&sink);
    auto jniWriter = std::make_unique<JniBufferedWriter>(fake.env(), output);
    NsparseStreamWriter writer(std::move(jniWriter));

    std::vector<int32_t> values = {1, 2, 3, 4, 5};
    writer.write(values.data(), sizeof(int32_t), values.size());
    writer.close();

    EXPECT_EQ(writer.pos(), values.size() * sizeof(int32_t));
    EXPECT_EQ(sink.size(), values.size() * sizeof(int32_t));
}

// The writer resolves IndexOutputWrapper.writeBytes reflectively, so a rename on
// the Java side surfaces only at runtime. Fail loudly at construction.
TEST_F(NsparseWrapperTest, JniBufferedWriterRejectsMissingWriteBytesMethod) {
    std::vector<char> sink;
    jobject output = fake.makeOutput(&sink);
    fake.missingMethodName = "writeBytes";

    EXPECT_THROW(JniBufferedWriter(fake.env(), output), std::runtime_error);
    // The simulated NoSuchMethodError must not be left pending for the caller.
    EXPECT_FALSE(fake.hasPendingException());
}

// A failing IndexOutput (disk full, closed directory) throws IOException on the
// Java side. The native error must carry that original cause, not replace it.
TEST_F(NsparseWrapperTest, StreamWriterPropagatesJavaExceptionCause) {
    std::vector<char> sink;
    jobject output = fake.makeOutput(&sink);
    auto jniWriter = std::make_unique<JniBufferedWriter>(fake.env(), output);
    NsparseStreamWriter writer(std::move(jniWriter));

    fake.writeBytesFailureMessage = "java.io.IOException: No space left on device";

    std::vector<char> payload = {'a', 'b', 'c'};
    writer.write(payload.data(), sizeof(char), payload.size());

    try {
        writer.close();  // triggers the flush that fails
        FAIL() << "expected the Java writeBytes failure to propagate";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("No space left on device"),
                  std::string::npos)
            << "original Java cause was lost: " << e.what();
    }
    // Cleared, so the next JNI call is not poisoned by a stale pending exception.
    EXPECT_FALSE(fake.hasPendingException());
}

// ---------------------------------------------------------------------------
// JNI entry points — exercise the exported Java_* symbols directly through the
// fake env so argument marshalling and result array construction are covered.
// ---------------------------------------------------------------------------

extern "C" {
JNIEXPORT jlong JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_initIndex(JNIEnv*, jclass, jlong,
                                                             jint, jobject);
JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_freeIndex(JNIEnv*, jclass, jlong);
JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_insertToIndex(
    JNIEnv*, jclass, jlong, jintArray, jlong, jlong, jlong, jint);
JNIEXPORT jobjectArray JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_queryIndex(
    JNIEnv*, jclass, jlong, jintArray, jfloatArray, jint, jobject);
JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_transferVectors(
    JNIEnv*, jclass, jlongArray, jintArray, jintArray, jfloatArray);
}

TEST_F(NsparseWrapperTest, JniInitIndexEntryPointMarshalsMap) {
    jobject map = fake.makeMap();
    fake.mapPut(map, "idmap", fake.makeBool(true));
    fake.mapPut(map, "index", fake.makeString("inverted"));

    jlong addr = Java_org_opensearch_neuralsearch_jni_NativeLibrary_initIndex(
        fake.env(), nullptr, 2, 16, map);
    ASSERT_NE(addr, 0);
    EXPECT_EQ(fake.throwCount, 0);
    Java_org_opensearch_neuralsearch_jni_NativeLibrary_freeIndex(fake.env(), nullptr,
                                                                 addr);
}

TEST_F(NsparseWrapperTest, JniInitIndexEntryPointThrowsOnMissingIndexKey) {
    jobject map = fake.makeMap();
    fake.mapPut(map, "idmap", fake.makeBool(true));  // no "index"

    jlong addr = Java_org_opensearch_neuralsearch_jni_NativeLibrary_initIndex(
        fake.env(), nullptr, 1, 8, map);
    EXPECT_EQ(addr, 0);
    // The C++ std::invalid_argument must have been translated to a Java throw.
    EXPECT_EQ(fake.throwCount, 1);
}

TEST_F(NsparseWrapperTest, JniTransferVectorsWritesAddressesBack) {
    jlongArray mem = fake.makeLongArray({0, 0, 0});
    jintArray indices = fake.makeIntArray({0, 2});
    jintArray tokens = fake.makeIntArray({3, 4});
    jfloatArray weights = fake.makeFloatArray({1.5f, 2.5f});

    Java_org_opensearch_neuralsearch_jni_NativeLibrary_transferVectors(
        fake.env(), nullptr, mem, indices, tokens, weights);
    EXPECT_EQ(fake.throwCount, 0);

    // The three off-heap addresses were written back into the long[].
    auto* memObj = FakeJniEnv::fo(reinterpret_cast<jobject>(mem));
    ASSERT_EQ(memObj->longs.size(), 3u);
    EXPECT_NE(memObj->longs[0], 0);
    EXPECT_NE(memObj->longs[1], 0);
    EXPECT_NE(memObj->longs[2], 0);

    // Every Get<Type>ArrayElements was matched by a Release, and the long[]
    // holding the addresses was released with copy-back (mode 0) rather than
    // JNI_ABORT — otherwise the addresses would never reach Java.
    EXPECT_EQ(fake.arrayElementsOutstanding, 0);
    EXPECT_EQ(fake.arrayElementsReleased, 4);

    delete reinterpret_cast<std::vector<int32_t>*>(memObj->longs[0]);
    delete reinterpret_cast<std::vector<int32_t>*>(memObj->longs[1]);
    delete reinterpret_cast<std::vector<float>*>(memObj->longs[2]);
}

// Regression: the entry points used to Get array elements and Release them only
// on the success path, so any throw from the wrapper stranded the pinned buffer
// for the life of the JVM.
TEST_F(NsparseWrapperTest, JniInsertToIndexReleasesArrayElementsOnThrow) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("inverted");
    int64_t index = wrapper::initIndex(1, 70000, params, fake.env());
    ASSERT_NE(index, 0);

    // Token 70000 exceeds term_t, so insertToIndex throws mid-call.
    OffHeapAddrs off = transfer({0, 1}, {70000}, {1.0f});
    jintArray ids = fake.makeIntArray({100});

    int releasedBefore = fake.arrayElementsReleased;
    Java_org_opensearch_neuralsearch_jni_NativeLibrary_insertToIndex(
        fake.env(), nullptr, index, ids, off.indices, off.tokens, off.values, 1);

    EXPECT_EQ(fake.throwCount, 1);
    EXPECT_EQ(fake.arrayElementsOutstanding, 0)
        << "the ids array was not released on the exception path";
    EXPECT_GT(fake.arrayElementsReleased, releasedBefore);

    wrapper::freeIndex(index);
}

// buildSparseQueryResults() is only reachable through this entry point. It sizes
// the returned array to the number of real hits and skips the -1 padding, so a
// short result set must not surface phantom doc 0 entries.
TEST_F(NsparseWrapperTest, JniQueryIndexBuildsResultsAndDropsPadding) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("inverted");
    int64_t index = wrapper::initIndex(2, 16, params, fake.env());
    ASSERT_NE(index, 0);

    // Two docs, both holding token 2.
    OffHeapAddrs off = transfer({0, 1, 2}, {2, 2}, {1.0f, 1.0f});
    std::vector<int32_t> ids = {7, 8};
    wrapper::insertToIndex(index, ids.data(), 2, off.indices, off.tokens,
                           off.values, 1);

    // Ask for k=5 when only 2 docs exist: 3 slots come back as padding.
    jintArray qTokens = fake.makeIntArray({2});
    jfloatArray qWeights = fake.makeFloatArray({1.0f});
    jobjectArray results = Java_org_opensearch_neuralsearch_jni_NativeLibrary_queryIndex(
        fake.env(), nullptr, index, qTokens, qWeights, /*k=*/5, nullptr);

    ASSERT_NE(results, nullptr);
    EXPECT_EQ(fake.throwCount, 0);
    auto* arr = FakeJniEnv::fo(reinterpret_cast<jobject>(results));
    // Exactly the real hits, not k.
    EXPECT_EQ(arr->objArray.size(), 2u);
    for (jobject entry : arr->objArray) {
        ASSERT_NE(entry, nullptr);
        auto* qr = FakeJniEnv::fo(entry);
        EXPECT_TRUE(qr->qrId == 7 || qr->qrId == 8)
            << "padding leaked into results as doc " << qr->qrId;
        EXPECT_GT(qr->qrScore, 0.0f);
    }
    EXPECT_EQ(fake.arrayElementsOutstanding, 0);

    wrapper::freeIndex(index);
}

// A query whose tokens are all unrepresentable produces no hits at all; the
// returned array must be empty rather than k entries of doc 0.
TEST_F(NsparseWrapperTest, JniQueryIndexReturnsEmptyArrayWhenNoHits) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("inverted");
    int64_t index = wrapper::initIndex(1, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 1}, {2}, {1.0f});
    std::vector<int32_t> ids = {100};
    wrapper::insertToIndex(index, ids.data(), 1, off.indices, off.tokens,
                           off.values, 1);

    jintArray qTokens = fake.makeIntArray({-1, 70000});
    jfloatArray qWeights = fake.makeFloatArray({1.0f, 1.0f});
    jobjectArray results = Java_org_opensearch_neuralsearch_jni_NativeLibrary_queryIndex(
        fake.env(), nullptr, index, qTokens, qWeights, /*k=*/3, nullptr);

    ASSERT_NE(results, nullptr);
    auto* arr = FakeJniEnv::fo(reinterpret_cast<jobject>(results));
    EXPECT_EQ(arr->objArray.size(), 0u);
    EXPECT_EQ(fake.throwCount, 0);

    wrapper::freeIndex(index);
}

// The methodParameters map reaches buildSearchParameters, which every other test
// skips by passing an empty map.
TEST_F(NsparseWrapperTest, JniQueryIndexAppliesSearchParameters) {
    std::map<std::string, jobject> params;
    params["idmap"] = fake.makeBool(true);
    params["index"] = fake.makeString("seismic");
    params["lambda"] = fake.makeNumber(10);
    params["beta"] = fake.makeNumber(5.0);
    params["alpha"] = fake.makeNumber(0.5);
    int64_t index = wrapper::initIndex(3, 16, params, fake.env());
    ASSERT_NE(index, 0);

    OffHeapAddrs off = transfer({0, 1, 2, 3}, {5, 5, 5}, {1.0f, 1.0f, 1.0f});
    std::vector<int32_t> ids = {10, 20, 30};
    wrapper::insertToIndex(index, ids.data(), 3, off.indices, off.tokens,
                           off.values, 1);

    jobject searchParams = fake.makeMap();
    fake.mapPut(searchParams, "cut", fake.makeNumber(4));
    fake.mapPut(searchParams, "heap_factor", fake.makeNumber(1.5));

    jintArray qTokens = fake.makeIntArray({5});
    jfloatArray qWeights = fake.makeFloatArray({1.0f});
    jobjectArray results = Java_org_opensearch_neuralsearch_jni_NativeLibrary_queryIndex(
        fake.env(), nullptr, index, qTokens, qWeights, /*k=*/3, searchParams);

    ASSERT_NE(results, nullptr);
    EXPECT_EQ(fake.throwCount, 0);
    auto* arr = FakeJniEnv::fo(reinterpret_cast<jobject>(results));
    EXPECT_GT(arr->objArray.size(), 0u);
    for (jobject entry : arr->objArray) {
        auto* qr = FakeJniEnv::fo(entry);
        EXPECT_TRUE(qr->qrId == 10 || qr->qrId == 20 || qr->qrId == 30);
    }

    wrapper::freeIndex(index);
}
