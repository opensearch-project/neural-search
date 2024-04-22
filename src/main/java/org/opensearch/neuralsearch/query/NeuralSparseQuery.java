/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * Implementation of Query interface for type NeuralSparseQuery when TwoPhaseNeuralSparse Enabled.
 * Initialized, it currentQuery include all tokenQuery. After call setCurrentQueryToHighScoreTokenQuery,
 * it will perform highScoreTokenQuery.
 */
@AllArgsConstructor
@Getter
@NonNull
public final class NeuralSparseQuery extends Query {

    private Query currentQuery;
    private final Query highScoreTokenQuery;
    private final Query lowScoreTokenQuery;
    private final Float rescoreWindowSizeExpansion;

    @Override
    public String toString(String field) {
        return "NeuralSparseQuery("
            + currentQuery.toString(field)
            + ","
            + highScoreTokenQuery.toString(field)
            + ", "
            + lowScoreTokenQuery.toString(field)
            + ")";

    }

    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query rewrittenCurrentQuery = currentQuery.rewrite(indexSearcher);
        Query rewrittenFirstStepQuery = highScoreTokenQuery.rewrite(indexSearcher);
        Query rewrittenSecondPhaseQuery = lowScoreTokenQuery.rewrite(indexSearcher);
        if (rewrittenFirstStepQuery == highScoreTokenQuery && rewrittenSecondPhaseQuery == lowScoreTokenQuery) {
            return this;
        }
        return new NeuralSparseQuery(rewrittenCurrentQuery, rewrittenFirstStepQuery, rewrittenSecondPhaseQuery, rescoreWindowSizeExpansion);
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        QueryVisitor v = queryVisitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
        currentQuery.visit(v);
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(NeuralSparseQuery other) {
        return Objects.equals(currentQuery, other.currentQuery)
            && Objects.equals(highScoreTokenQuery, other.highScoreTokenQuery)
            && Objects.equals(lowScoreTokenQuery, other.lowScoreTokenQuery)
            && Objects.equals(rescoreWindowSizeExpansion, other.rescoreWindowSizeExpansion);
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + Objects.hashCode(highScoreTokenQuery) + Objects.hashCode(lowScoreTokenQuery) + Objects.hashCode(currentQuery) + Objects
            .hashCode(rescoreWindowSizeExpansion);
        return h;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return currentQuery.createWeight(searcher, scoreMode, boost);
    }

    /**
     * Before call this function, the currentQuery of this object is allTokenQuery.
     * After call this function, the currentQuery of this object change to highScoreTokenQuery.
     */
    public void setCurrentQueryToHighScoreTokenQuery() {
        this.currentQuery = highScoreTokenQuery;
    }
}
