/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.rankdocs;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * Lucene {@link Query} that scores a fused window directly from parallel {@code docIds}/{@code scores}
 * arrays via an O(window) sorted-int scan, stamping each doc's fused score as its {@code _score}. This is the optional
 * {@code (shardIndex, docId)} / PIT fast-path scorer; the default {@code _id}-keyed self-erase path
 * ({@link org.opensearch.neuralsearch.query.HybridFusionQuery}, a {@code constant_score(IdsQuery)} bool) does not need
 * it, so this class is <b>not wired to execution in PR1</b>.
 *
 * <p>Kept small and dependency-light (no {@code Writeable}, no plugin-specific types) so it is a clean candidate for
 * the retriever framework to lift into OpenSearch core later; the resolver would then adopt the core class and retire
 * this copy. The scoring {@link Weight}/iterator implementation is deferred to PR5, where the PIT/docId fast path is
 * introduced — {@link #createWeight} intentionally throws until then.
 */
public final class FusedDocsScorerQuery extends Query {

    private final int[] docIds;
    private final float[] scores;

    /**
     * @param docIds Lucene-local doc ids for this shard, ascending (the sorted-int scan relies on ordering)
     * @param scores fused scores positionally aligned to {@code docIds}
     */
    public FusedDocsScorerQuery(int[] docIds, float[] scores) {
        if (Objects.isNull(docIds) || Objects.isNull(scores) || docIds.length != scores.length) {
            throw new IllegalArgumentException("docIds and scores must be non-null and of equal length");
        }
        this.docIds = docIds;
        this.scores = scores;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        throw new UnsupportedOperationException(
            String.format(
                Locale.ROOT,
                "[%s] scoring is not implemented yet (docId/PIT fast path lands in a later change)",
                getClass().getSimpleName()
            )
        );
    }

    @Override
    public String toString(String field) {
        return String.format(Locale.ROOT, "FusedDocsScorerQuery(fusedDocs=%d)", docIds.length);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        FusedDocsScorerQuery other = (FusedDocsScorerQuery) obj;
        return Arrays.equals(docIds, other.docIds) && Arrays.equals(scores, other.scores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), Arrays.hashCode(docIds), Arrays.hashCode(scores));
    }
}
