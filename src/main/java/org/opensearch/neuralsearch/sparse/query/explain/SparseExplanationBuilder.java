/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query.explain;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.search.Query;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
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
