/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.Builder;
import lombok.Getter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;
import java.util.List;

/**
 * A new query type for "sparse_ann" query. It vends a customized weight and scorer so that it can utilize
 * posting clustering algorithms.
 */
@Getter
public class SparseVectorQuery extends Query {

    private final SparseVector queryVector;
    private final List<String> tokens;
    private final String fieldName;

    @Builder
    public SparseVectorQuery(SparseVector queryVector, String fieldName, List<String> tokens) {
        this.queryVector = queryVector;
        this.fieldName = fieldName;
        this.tokens = tokens;
    }

    @Override
    public String toString(String field) {
        return "SparseVectorQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {

    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SparseVectorQuery queryVectorQuery = (SparseVectorQuery) obj;
        return queryVector.equals(queryVectorQuery.getQueryVector());
    }

    @Override
    public int hashCode() {
        return queryVector.hashCode();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new SparseQueryWeight(this);
    }
}
