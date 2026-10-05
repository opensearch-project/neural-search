/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query.explain;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.Query;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.BinaryVectorUtils;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.query.SparseVectorQuery;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Builder class for constructing detailed explanations of Sparse ANN query scoring.
 *
 * This class orchestrates the construction of multi-level explanations that break down
 * the SEISMIC algorithm's scoring process, including:
 * - Query token pruning - 'top_n' tokens with the highest weight will be kept
 * - Raw dot product score calculation with token-level contributions
 * - Quantization rescaling - Convert Byte format data back into float
 * - Filter application
 *
 * The score is recomputed from the document's stored vector rather than recorded during search, so
 * it reproduces the scorer's arithmetic: the quantized path matches nsparse's decode_dot_product,
 * which is the same `raw * ceiling_ingest * ceiling_search / 255 / 255` this class applies.
 *
 * Known limit on the quantized path: {@link SparseVector} folds token ids modulo
 * {@code MODULUS_FOR_SHORT} (32768) and merges collisions by max weight, while the native engine
 * indexes raw ids up to 65535. A field using ids at or above 32768 therefore aliases tokens here
 * and can report a score the native scorer did not produce.
 */
@Log4j2
@Builder
public class SparseExplanationBuilder {

    @NonNull
    private final LeafReaderContext context;

    private final int docId;

    @NonNull
    private final SparseVectorQuery query;

    private final float boost;

    @NonNull
    private final FieldInfo fieldInfo;

    @NonNull
    private final SparseVectorReader reader;

    /**
     * Whether the segment was scored by the native engine. Only changes how the search mode is
     * described: native hands a filter to nsparse as a candidate set, where Lucene post-filters.
     */
    private final boolean nativeEngine;

    /**
     * Set instead of {@link #reader} for a segment scored over unquantized floats -- nsparse's
     * inverted index, used below the approximate threshold. Reading the doc values directly rather
     * than through a {@link SparseVectorReader} is what keeps the weights unquantized.
     */
    private final BinaryDocValues rawDocValues;

    /**
     * Constructs a complete explanation for the document's score.
     * @return A Lucene Explanation object containing the complete scoring breakdown
     */
    public Explanation explain() {
        if (docId < 0) {
            return Explanation.noMatch(String.format(Locale.ROOT, "invalid document ID %d (must be non-negative)", docId));
        }
        if (query.getQueryVector().getSize() == 0) {
            return Explanation.noMatch(String.format(Locale.ROOT, "query vector is empty or null for field '%s'", query.getFieldName()));
        }
        if (rawDocValues != null) {
            return explainExactFloatScore();
        }

        SparseVector docVector;
        try {
            docVector = reader.read(docId);
        } catch (IOException e) {
            return Explanation.noMatch(
                String.format(Locale.ROOT, "error reading document %d in field '%s': %s", docId, query.getFieldName(), e.getMessage())
            );
        }
        if (docVector == null) {
            return Explanation.noMatch(
                String.format(Locale.ROOT, "document %d not found or has no sparse vector in field '%s'", docId, query.getFieldName())
            );
        }

        // Calculate raw dot product score
        byte[] queryDenseVector = query.getQueryVector().toDenseVector();
        int rawScore = docVector.dotProduct(queryDenseVector);

        // Build explanation components
        List<Explanation> details = new ArrayList<>();
        details.add(explainQueryPruning());
        details.add(explainRawScore(rawScore, docVector, queryDenseVector));
        details.add(explainQuantizationRescaling());

        // Add filter explanation if filter is present
        if (query.getFilter() != null) {
            details.add(explainFilter());
        }

        // Calculate final score with quantization rescaling
        float rescaledBoost = calculateRescaledBoost();
        float finalScore = rawScore * rescaledBoost;

        return Explanation.match(
            finalScore,
            String.format(Locale.ROOT, "sparse_ann score for doc %d in field '%s'", docId, query.getFieldName()),
            details
        );
    }

    /**
     * Explains a score produced over unquantized floats, which is what nsparse's inverted index
     * computes for a segment below the approximate threshold. There is no quantization step to
     * describe and no rescaling to undo: the score is the float dot product times the boost.
     *
     * @return A Lucene Explanation for the exact float scoring path
     */
    private Explanation explainExactFloatScore() {
        Map<Integer, Float> docWeights;
        try {
            if (rawDocValues.advanceExact(docId) == false) {
                return Explanation.noMatch(
                    String.format(Locale.ROOT, "document %d has no sparse vector in field '%s'", docId, query.getFieldName())
                );
            }
            BytesRef bytesRef = rawDocValues.binaryValue();
            if (bytesRef == null) {
                return Explanation.noMatch(
                    String.format(Locale.ROOT, "document %d has no sparse vector in field '%s'", docId, query.getFieldName())
                );
            }
            docWeights = BinaryVectorUtils.readToMap(bytesRef);
        } catch (IOException e) {
            return Explanation.noMatch(
                String.format(Locale.ROOT, "error reading document %d in field '%s': %s", docId, query.getFieldName(), e.getMessage())
            );
        }

        // The native scorer is handed the raw query weights, so the dot product is over the query as
        // the user sent it, not the pruned token list -- pruning only picks posting lists to visit.
        Map<Integer, Float> queryWeights = query.getRawQueryTokens();
        List<Explanation> tokenDetails = new ArrayList<>();
        float rawScore = 0.0f;
        List<Map.Entry<Integer, Float>> contributions = new ArrayList<>();
        for (Map.Entry<Integer, Float> queryEntry : queryWeights.entrySet()) {
            Float docWeight = docWeights.get(queryEntry.getKey());
            if (docWeight == null) {
                continue;
            }
            float contribution = queryEntry.getValue() * docWeight;
            rawScore += contribution;
            contributions.add(Map.entry(queryEntry.getKey(), contribution));
        }
        contributions.sort(Map.Entry.<Integer, Float>comparingByValue().reversed());
        for (Map.Entry<Integer, Float> contribution : contributions) {
            int token = contribution.getKey();
            tokenDetails.add(
                Explanation.match(
                    contribution.getValue(),
                    String.format(
                        Locale.ROOT,
                        "token '%d' contribution: query_weight=%f * doc_weight=%f",
                        token,
                        queryWeights.get(token),
                        docWeights.get(token)
                    )
                )
            );
        }

        List<Explanation> details = new ArrayList<>();
        details.add(explainQueryPruning());
        details.add(Explanation.match(rawScore, String.format(Locale.ROOT, "dot product score (exact): %f", rawScore), tokenDetails));
        details.add(Explanation.match(boost, String.format(Locale.ROOT, "boost: %.4f", boost)));
        if (query.getFilter() != null) {
            details.add(explainFilter());
        }

        return Explanation.match(
            rawScore * boost,
            String.format(
                Locale.ROOT,
                "sparse_ann score for doc %d in field '%s' (exact scoring, segment below approximate_threshold)",
                docId,
                query.getFieldName()
            ),
            details
        );
    }

    /**
     * Creates an explanation for the raw dot product score calculation.
     * @param rawScore The calculated raw dot product score
     * @param docVector The document's sparse vector
     * @param queryDenseVector The query vector in dense format
     * @return An Explanation showing the raw score and token-level contributions
     */
    private Explanation explainRawScore(int rawScore, SparseVector docVector, byte[] queryDenseVector) {
        List<Explanation> tokenDetails = new ArrayList<>();
        List<String> queryTokens = query.getQueryContext().getTokens();

        // Calculate contribution for each query token
        List<TokenContribution> contributions = new ArrayList<>();
        for (String tokenStr : queryTokens) {
            int tokenId;
            try {
                tokenId = Integer.parseInt(tokenStr);
            } catch (NumberFormatException e) {
                log.warn("Invalid token ID '{}' in query context, skipping", tokenStr);
                continue;
            }

            if (tokenId < 0 || tokenId >= queryDenseVector.length) {
                continue;
            }

            byte queryWeight = queryDenseVector[tokenId];

            if (queryWeight == 0) {
                continue;
            }

            byte docWeight = 0;
            SparseVector.Item item;
            var iterator = docVector.iterator();
            while ((item = iterator.next()) != null) {
                if (item.getToken() == SparseVector.prepareTokenForShortType(tokenId)) {
                    docWeight = item.getWeight();
                    break;
                }
            }

            // Only include tokens that appear in both query and document
            if (docWeight != 0) {
                int contribution = ByteQuantizationUtil.multiplyUnsignedByte(queryWeight, docWeight);
                contributions.add(new TokenContribution(tokenStr, queryWeight, docWeight, contribution));
            }
        }

        contributions.sort(Comparator.comparingInt(TokenContribution::getContribution).reversed());

        for (TokenContribution tc : contributions) {
            tokenDetails.add(
                Explanation.match(
                    tc.getContribution(),
                    String.format(
                        Locale.ROOT,
                        "token '%s' contribution: query_weight=%d * doc_weight=%d",
                        tc.getToken(),
                        ByteQuantizationUtil.getUnsignedByte(tc.getQueryWeight()),
                        ByteQuantizationUtil.getUnsignedByte(tc.getDocWeight())
                    )
                )
            );
        }

        return Explanation.match(rawScore, String.format(Locale.ROOT, "raw dot product score (quantized): %d", rawScore), tokenDetails);
    }

    /**
     * Calculates the rescaled boost factor based on quantization parameters.
     * The rescaled boost accounts for the quantization of both ingestion and search vectors.
     * When vectors are quantized from float (0-ceiling) to byte (0-255), the dot product
     * needs to be rescaled to match the original float scale.
     * Formula: boost * ceiling_ingest * ceiling_search / 255 / 255
     * @return The rescaled boost factor to apply to the raw quantized score
     */
    private float calculateRescaledBoost() {
        float ceilingIngest = ByteQuantizationUtil.getCeilingValueIngest(fieldInfo);
        float ceilingSearch = ByteQuantizationUtil.getCeilingValueSearch(fieldInfo);
        return boost * ceilingIngest * ceilingSearch / ByteQuantizationUtil.MAX_UNSIGNED_BYTE_VALUE
            / ByteQuantizationUtil.MAX_UNSIGNED_BYTE_VALUE;
    }

    /**
     * Creates an explanation for the quantization rescaling calculation.
     * @return An Explanation showing the rescaling formula and parameter values
     */
    private Explanation explainQuantizationRescaling() {
        float ceilingIngest = ByteQuantizationUtil.getCeilingValueIngest(fieldInfo);
        float ceilingSearch = ByteQuantizationUtil.getCeilingValueSearch(fieldInfo);
        float rescaledBoost = calculateRescaledBoost();

        List<Explanation> details = new ArrayList<>();
        details.add(Explanation.match(boost, String.format(Locale.ROOT, "original boost: %.4f", boost)));
        details.add(
            Explanation.match(ceilingIngest, String.format(Locale.ROOT, "ceiling_ingest (quantization parameter): %.2f", ceilingIngest))
        );
        details.add(
            Explanation.match(ceilingSearch, String.format(Locale.ROOT, "ceiling_search (quantization parameter): %.2f", ceilingSearch))
        );
        details.add(
            Explanation.match(
                ByteQuantizationUtil.MAX_UNSIGNED_BYTE_VALUE,
                String.format(Locale.ROOT, "MAX_UNSIGNED_BYTE_VALUE: %d", ByteQuantizationUtil.MAX_UNSIGNED_BYTE_VALUE)
            )
        );

        return Explanation.match(
            rescaledBoost,
            String.format(
                Locale.ROOT,
                "quantization rescaling: %.4f * %.2f * %.2f / %d / %d = %.6f",
                boost,
                ceilingIngest,
                ceilingSearch,
                ByteQuantizationUtil.MAX_UNSIGNED_BYTE_VALUE,
                ByteQuantizationUtil.MAX_UNSIGNED_BYTE_VALUE,
                rescaledBoost
            ),
            details
        );
    }

    /**
     * Creates an explanation for query token pruning (only top-N tokens kept).
     * @return An Explanation showing the query pruning details
     */
    private Explanation explainQueryPruning() {
        int originalTokenCount = query.getQueryVector().getSize();
        int prunedTokenCount = query.getQueryContext().getTokens().size();

        if (originalTokenCount == prunedTokenCount) {
            return Explanation.match(
                prunedTokenCount,
                String.format(Locale.ROOT, "query token pruning: kept all %d tokens (no pruning occurred)", prunedTokenCount)
            );
        }

        return Explanation.match(
            prunedTokenCount,
            String.format(Locale.ROOT, "query token pruning: kept top %d of %d tokens", prunedTokenCount, originalTokenCount)
        );
    }

    /**
     * Creates an explanation for filter application.
     *
     * This method checks whether the document passed the filter and explains which
     * search mode was used based on the relationship between P (filtered document count)
     * and k (requested result count):
     *
     * - When P <= k: Exact search mode with pre-filtering. All filtered documents are
     *   scored exactly using brute-force dot product calculation.
     * - When P > k: Approximate search mode with post-filtering. ANN search runs first
     *   on all documents, then filter is applied to the results.
     *
     * The explanation includes the filter query description to help users understand
     * what filtering criteria was applied.
     *
     * @return An Explanation indicating whether the document passed the filter and which search mode was used
     */
    private Explanation explainFilter() {
        Map<Object, BitSet> filterResults = query.getFilterResults();

        if (filterResults == null) {
            return Explanation.match(1.0f, "filter present but no filter results available");
        }

        BitSet bitSet = filterResults.get(context.id());

        if (bitSet == null) {
            return Explanation.noMatch("document filtered out (no documents in segment matched filter)");
        }

        Query filterQuery = query.getFilter();
        List<Explanation> details = new ArrayList<>();

        if (filterQuery != null) {
            details.add(Explanation.match(1.0f, String.format(Locale.ROOT, "filter criteria: %s", filterQuery)));
        }

        if (bitSet.get(docId)) {
            int passedCount = bitSet.cardinality();
            int k = query.getQueryContext().getK();

            if (passedCount <= k) {
                // Exact search mode: pre-filtering, all filtered documents scored exactly
                return Explanation.match(
                    1.0f,
                    String.format(
                        Locale.ROOT,
                        "document passed filter with exact search mode "
                            + "(filter matched %d documents <= k=%d, all filtered documents scored exactly)",
                        passedCount,
                        k
                    ),
                    details
                );
            } else if (nativeEngine) {
                // The native engine hands the filter to nsparse as a candidate set, so the ANN
                // search is restricted to it rather than filtered afterwards.
                return Explanation.match(
                    1.0f,
                    String.format(
                        Locale.ROOT,
                        "document passed filter with approximate search mode "
                            + "(filter matched %d documents > k=%d, ANN search restricted to the filtered documents)",
                        passedCount,
                        k
                    ),
                    details
                );
            } else {
                // Approximate search mode: ANN search first, then post-filtering
                return Explanation.match(
                    1.0f,
                    String.format(
                        Locale.ROOT,
                        "document passed filter with approximate search mode "
                            + "(filter matched %d documents > k=%d, ANN search performed first then filtered)",
                        passedCount,
                        k
                    ),
                    details
                );
            }
        } else {
            return Explanation.noMatch("document filtered out (did not match filter criteria, filter multiplier: 0.0)", details);
        }
    }

    /**
     * Internal class to hold token contribution details.
     */
    @Value
    private static class TokenContribution {
        String token;
        byte queryWeight;
        byte docWeight;
        int contribution;
    }
}
