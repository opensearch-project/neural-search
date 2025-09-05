/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * A new query type for "SEISMIC" query. It vends a customized weight and scorer so that it can work on
 * SEISMIC's clustering posting data structure.
 */
@Getter
@Builder
@AllArgsConstructor
public class SparseVectorQuery extends Query {
    @NonNull
    private final SparseVector queryVector;
    @NonNull
    private final SparseQueryContext queryContext;
    @NonNull
    private final String fieldName;
    @NonNull
    private final Query fallbackQuery;
    private final Query filter;
    private Map<Object, BitSet> filterResults;

    @Override
    public String toString(String field) {
        return field;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(getFieldName())) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SparseVectorQuery otherQuery = (SparseVectorQuery) obj;
        if (!this.queryContext.equals(otherQuery.queryContext)) {
            return false;
        }
        if (!this.fieldName.equals(otherQuery.fieldName)) {
            return false;
        }
        if (!this.fallbackQuery.equals(otherQuery.getFallbackQuery())) {
            return false;
        }
        if ((this.filter == null) != (otherQuery.getFilter() == null)) {
            return false;
        }
        if (this.filter != null && !this.filter.equals(otherQuery.getFilter())) {
            return false;
        }
        return queryVector.equals(otherQuery.getQueryVector());
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryVector, queryContext, fieldName, fallbackQuery, filter);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        IndexReader reader = indexSearcher.getIndexReader();
        final Weight filterWeight = createFilterWeight(indexSearcher);
        if (filterWeight == null) {
            return this;
        }
        TaskExecutor taskExecutor = indexSearcher.getTaskExecutor();
        List<LeafReaderContext> leafReaderContexts = reader.leaves();
        List<Callable<Map.Entry<Object, BitSet>>> tasks = new ArrayList<>(leafReaderContexts.size());
        for (LeafReaderContext context : leafReaderContexts) {
            tasks.add(() -> runFilter(context, filterWeight));
        }
        Map.Entry<Object, BitSet>[] results = taskExecutor.invokeAll(tasks).toArray(Map.Entry[]::new);
        this.filterResults = new HashMap<>();
        for (Map.Entry<Object, BitSet> filterResult : results) {
            if (filterResult != null) {
                filterResults.put(filterResult.getKey(), filterResult.getValue());
            }
        }
        return this;
    }

    private Map.Entry<Object, BitSet> runFilter(LeafReaderContext ctx, Weight filterWeight) throws IOException {
        final LeafReader reader = ctx.reader();
        Scorer scorer = filterWeight.scorer(ctx);
        if (scorer == null) {
            return null;
        }

        return Map.entry(ctx.id(), createBitSet(scorer.iterator(), reader.getLiveDocs(), reader.maxDoc()));
    }

    @VisibleForTesting
    BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc) throws IOException {
        if (liveDocs == null && iterator instanceof BitSetIterator bitSetIterator) {
            // If we already have a BitSet and no deletions, reuse the BitSet
            return bitSetIterator.getBitSet();
        } else {
            // Create a new BitSet from matching and live docs
            FilteredDocIdSetIterator filterIterator = new FilteredDocIdSetIterator(iterator) {
                @Override
                protected boolean match(int doc) {
                    return liveDocs == null || liveDocs.get(doc);
                }
            };
            return BitSet.of(filterIterator, maxDoc);
        }
    }

    private Weight createFilterWeight(IndexSearcher indexSearcher) throws IOException {
        Weight filterWeight = null;
        if (filter != null) {
            BooleanQuery booleanQuery = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER)
                .add(new FieldExistsQuery(fieldName), BooleanClause.Occur.FILTER)
                .build();
            Query rewritten = indexSearcher.rewrite(booleanQuery);
            filterWeight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
        }
        return filterWeight;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new SparseQueryWeight(this, searcher, scoreMode, boost, ForwardIndexCache.getInstance());
    }
}
