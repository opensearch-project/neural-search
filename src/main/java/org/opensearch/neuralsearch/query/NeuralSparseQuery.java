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

    /**
     *
     * @param field The field of this query.
     * @return String of NeuralSparseQuery object.
     */
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
        if (rewrittenCurrentQuery == currentQuery
            && rewrittenFirstStepQuery == highScoreTokenQuery
            && rewrittenSecondPhaseQuery == lowScoreTokenQuery) {
            return this;
        }
        return new NeuralSparseQuery(rewrittenCurrentQuery, rewrittenFirstStepQuery, rewrittenSecondPhaseQuery, rescoreWindowSizeExpansion);
    }

    /**
     * This interface is let the lucene to visit this query.
     * Briefly, the query to be performed always be a subset of current query.
     * So in this function use currentQuery.visit().
     * @param queryVisitor a QueryVisitor to be called by each query in the tree
     */
    @Override
    public void visit(QueryVisitor queryVisitor) {
        currentQuery.visit(queryVisitor);
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

    /**
     * This function is always performed after setCurrentQueryToHighScoreTokenQuery. And determine which query's weight to score docs.
     * @param searcher The searcher that execute the neural_sparse query.
     * @param scoreMode How the produced scorers will be consumed.
     * @param boost The boost that is propagated by the parent queries.
     * @return The weight of currentQuery.
     * @throws IOException If creteWeight failed.
     */
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return currentQuery.createWeight(searcher, scoreMode, boost);
    }

    /**
     * Before call this function, the currentQuery of this object is a BooleanQuery include all tokens FeatureFiledQuery.
     * After call this function, the currentQuery of this object change to a BooleanQuery include high score tokens FeatureFiledQuery.
     */
    public void setCurrentQueryToHighScoreTokenQuery() {
        this.currentQuery = highScoreTokenQuery;
    }
}
