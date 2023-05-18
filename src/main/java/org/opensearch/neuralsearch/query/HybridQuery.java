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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * Implementation fo Query interface for type "hybrid". It allows execution of multiple sub-queries and collect individual
 * scores for each sub-query.
 *
 * @opensearch.internal
 */
public class HybridQuery extends Query implements Iterable<Query> {

    private final List<Query> subQueries;

    public HybridQuery(Collection<Query> subQueries) {
        Objects.requireNonNull(subQueries, "Collection of Queries must not be null");
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
            } else buffer.append(subquery.toString(field));
            if (i != subQueries.size() - 1) buffer.append(" | ");
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

        if (subQueries.size() == 1) {
            return subQueries.iterator().next();
        }

        boolean actuallyRewritten = false;
        List<Query> rewrittenSubQueries = new ArrayList<>();
        for (Query subQuery : subQueries) {
            Query rewrittenSub = subQuery.rewrite(reader);
            actuallyRewritten |= rewrittenSub != subQuery;
            rewrittenSubQueries.add(rewrittenSub);
        }

        if (actuallyRewritten) {
            return new HybridQuery(rewrittenSubQueries);
        }

        return super.rewrite(reader);
    }

    /**
     * Recurse through the query tree, visiting any child queries
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
     *  Calculate query weights and build query scorers for hybrid query.
     */
    protected class HybridQueryWeight extends Weight {

        // The Weights for our subqueries, in 1-1 correspondence
        protected final ArrayList<Weight> weights;

        private final ScoreMode scoreMode;

        /**
         * Construct the Weight for this Query searched by searcher. Recursively construct subquery weights.
         */
        public HybridQueryWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            super(HybridQuery.this);
            weights = new ArrayList<>();
            for (Query query : subQueries) {
                weights.add(searcher.createWeight(query, scoreMode, boost));
            }
            this.scoreMode = scoreMode;
        }

        /**
         * Returns Matches for a specific document, or null if the document does not match the parent query
         * @param context the reader's context to create the {@link Matches} for
         * @param doc the document's id relative to the given context's reader
         * @return
         * @throws IOException
         */
        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            List<Matches> mis = new ArrayList<>();
            for (Weight weight : weights) {
                Matches mi = weight.matches(context, doc);
                if (mi != null) {
                    mis.add(mi);
                }
            }
            return MatchesUtils.fromSubMatches(mis);
        }

        /**
         * Create the scorer used to score our associated Query
         * @param context the {@link org.apache.lucene.index.LeafReaderContext} for which to return the
         *     {@link Scorer}.
         * @return scorer of hybrid query that contains scorers of each sub-query
         * @throws IOException
         */
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer[] scorers = new Scorer[weights.size()];
            for (int i = 0; i < weights.size(); i++) {
                Weight w = weights.get(i);
                // we will advance() subscorers
                Scorer subScorer = w.scorer(context);
                if (subScorer != null) {
                    scorers[i] = subScorer;
                }
            }
            return new HybridQueryScorer(this, scorers);
        }

        /**
         * Check if weight object can be cached
         * @param ctx
         * @return true if the object can be cached against a given leaf
         */
        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            for (Weight w : weights) {
                if (!w.isCacheable(ctx)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Explain is not supported for hybrid query
         *
         * @param context the readers context to create the {@link Explanation} for.
         * @param doc the document's id relative to the given context's reader
         * @return
         * @throws IOException
         */
        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            throw new UnsupportedOperationException("Explain is not supported");
        }
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
        return new HybridQueryWeight(searcher, scoreMode, boost);
    }
}
