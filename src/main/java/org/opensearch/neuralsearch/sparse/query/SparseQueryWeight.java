/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.neuralsearch.sparse.algorithm.ByteQuantizer;
import org.opensearch.neuralsearch.sparse.codec.InMemorySparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;

/**
 * Weight class for SparseVectorQuery
 */
@Log4j2
public class SparseQueryWeight extends Weight {
    private final float boost;
    private final Weight booleanQueryWeight;

    public SparseQueryWeight(SparseVectorQuery query, IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        super(query);
        this.boost = boost;
        this.booleanQueryWeight = query.getOriginalQuery().createWeight(searcher, scoreMode, boost);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return null;
    }

    private SegmentInfo getSegmentInfo(LeafReader reader) {
        LeafReader leafReader = reader;
        while (leafReader != null && !(leafReader instanceof SegmentReader)) {
            if (leafReader instanceof FilterLeafReader) {
                leafReader = ((FilterLeafReader) leafReader).getDelegate();
            } else if (leafReader instanceof FilterCodecReader) {
                leafReader = ((FilterCodecReader) leafReader).getDelegate();
            }
        }
        if (!(leafReader instanceof SegmentReader)) {
            return null;
        }
        return ((SegmentReader) leafReader).getSegmentInfo().info;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final SparseVectorQuery query = (SparseVectorQuery) parentQuery;
        SegmentInfo info = getSegmentInfo(context.reader());
        String fieldType = query.getFieldName();
        FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(fieldType);
        // fallback to plain neural sparse query
        if (!PredicateUtils.shouldRunSeisPredicate.test(info, fieldInfo)) {
            return booleanQueryWeight.scorerSupplier(context);
        }
        final Scorer scorer = selectScorer(query, context, info, fieldType);
        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                return scorer;
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                return new BulkScorer() {
                    // We ignore the max value as our algorithm can't limit the docId to range of (min, max)
                    // so, to ensure it's only called once, we return the maxDoc
                    @Override
                    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                        collector.setScorer(scorer);
                        DocIdSetIterator iter = scorer.iterator();
                        int docId = iter.nextDoc();
                        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                            collector.collect(docId);
                            docId = iter.nextDoc();
                        }
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }

                    @Override
                    public long cost() {
                        return 0;
                    }
                };
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    private Scorer selectScorer(SparseVectorQuery query, LeafReaderContext context, SegmentInfo segmentInfo, String fieldType)
        throws IOException {
        SparseVectorReader sparseReader = null;
        if (segmentInfo != null) {
            InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(segmentInfo, fieldType);
            SparseVectorForwardIndex index = InMemorySparseVectorForwardIndex.get(key);
            if (index != null) {
                sparseReader = index.getReader();
            }
        }
        Similarity.SimScorer simScorer = ByteQuantizer.getSimScorer(boost);
        BitSetIterator filterBitIterator = null;
        if (query.getFilterResults() != null) {
            BitSet filter = query.getFilterResults().get(context.id());
            if (filter != null) {
                int ord = filter.cardinality();
                filterBitIterator = new BitSetIterator(filter, ord);
                if (ord <= query.getQueryContext().getK()) {
                    return new ExactMatchScorer(filterBitIterator, query.getQueryVector(), sparseReader, simScorer);
                }
            }
        }
        return new OrderedPostingWithClustersScorer(
            query.getFieldName(),
            query.getQueryContext(),
            query.getQueryVector(),
            context.reader(),
            context.reader().getLiveDocs(),
            sparseReader,
            simScorer,
            filterBitIterator
        );
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }
}
