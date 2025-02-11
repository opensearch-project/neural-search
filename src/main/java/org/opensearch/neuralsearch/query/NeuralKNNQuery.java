/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Getter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * Wraps KNN Lucene query to support neural search extensions.
 * Delegates core operations to the underlying KNN query.
 */
@Getter
public class NeuralKNNQuery extends Query {
    private final Query knnQuery;

    public NeuralKNNQuery(Query knnQuery) {
        this.knnQuery = knnQuery;
    }

    @Override
    public String toString(String field) {
        return knnQuery.toString(field);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        // Delegate the visitor to the underlying KNN query
        knnQuery.visit(visitor);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        // Delegate weight creation to the underlying KNN query
        return knnQuery.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = knnQuery.rewrite(reader);
        if (rewritten == knnQuery) {
            return this;
        }
        return new NeuralKNNQuery(rewritten);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        NeuralKNNQuery that = (NeuralKNNQuery) other;
        return Objects.equals(knnQuery, that.knnQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(knnQuery);
    }
}
