/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * Implementation of Query interface for type "hybrid". It allows execution of multiple sub-queries and collect individual
 * scores for each sub-query.
 */
public final class HybridQuery extends Query implements Iterable<Query> {

    private final List<Query> subQueries;

    public HybridQuery(Collection<Query> subQueries) {
        Objects.requireNonNull(subQueries, "collection of queries must not be null");
        if (subQueries.isEmpty()) {
            throw new IllegalArgumentException("collection of queries must not be empty");
        }
        this.subQueries = new ArrayList<>(subQueries);
    }

    /**
     * Returns an iterator over sub-queries that are parts of this hybrid query
     * @return iterator
     */
    @Override
    public Iterator<Query> iterator() {
        return getSubQueries().iterator();
    }

    /**
     * Prints a query to a string, with field assumed to be the default field and omitted.
     * @param field default field
     * @return string representation of hybrid query
     */
    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("(");
        Iterator<Query> it = subQueries.iterator();
        for (int i = 0; it.hasNext(); i++) {
            Query subquery = it.next();
            if (subquery instanceof BooleanQuery) { // wrap sub-boolean in parents
                buffer.append("(");
                buffer.append(subquery.toString(field));
                buffer.append(")");
            } else {
                buffer.append(subquery.toString(field));
            }
            if (i != subQueries.size() - 1) {
                buffer.append(" | ");
            }
        }
        buffer.append(")");
        return buffer.toString();
    }

    /**
     * Re-writes queries into primitive queries. Callers are expected to call rewrite multiple times if necessary,
     * until the rewritten query is the same as the original query.
     * @param reader
     * @return
     * @throws IOException
     */
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (subQueries.isEmpty()) {
            return new MatchNoDocsQuery("empty HybridQuery");
        }

        boolean actuallyRewritten = false;
        List<Query> rewrittenSubQueries = new ArrayList<>();
        for (Query subQuery : subQueries) {
            Query rewrittenSub = subQuery.rewrite(reader);
            /* we keep rewrite sub-query unless it's not equal to itself, it may take multiple levels of recursive calls
               queries need to be rewritten from high-level clauses into lower-level clauses because low-level clauses
               perform better. For hybrid query we need to track progress of re-write for all sub-queries */
            actuallyRewritten |= rewrittenSub != subQuery;
            rewrittenSubQueries.add(rewrittenSub);
        }

        if (actuallyRewritten) {
            return new HybridQuery(rewrittenSubQueries);
        }

        return super.rewrite(reader);
    }

    /**
     * Recurse through the query tree, visiting all child queries and execute provided visitor. Part of multiple
     * standard workflows, e.g. IndexSearcher.rewrite
     * @param queryVisitor a QueryVisitor to be called by each query in the tree
     */
    @Override
    public void visit(QueryVisitor queryVisitor) {
        QueryVisitor v = queryVisitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
        for (Query q : subQueries) {
            q.visit(v);
        }
    }

    /**
     * Override and implement query instance equivalence properly in a subclass. This is required so that QueryCache works properly.
     * @param other query object that when compare with this query object
     * @return
     */
    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(HybridQuery other) {
        return Objects.equals(subQueries, other.subQueries);
    }

    /**
     * Override and implement query hash code properly in a subclass. This is required so that QueryCache works properly.
     * @return hash code of this object
     */
    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + Objects.hashCode(subQueries);
        return h;
    }

    public Collection<Query> getSubQueries() {
        return Collections.unmodifiableCollection(subQueries);
    }

    /**
     * Create the Weight used to score this query
     *
     * @param searcher
     * @param scoreMode How the produced scorers will be consumed.
     * @param boost The boost that is propagated by the parent queries.
     * @return
     * @throws IOException
     */
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new HybridQueryWeight(this, searcher, scoreMode, boost);
    }
}
