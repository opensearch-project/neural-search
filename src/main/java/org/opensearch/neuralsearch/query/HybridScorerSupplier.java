/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class is responsible for creating a HybridScorer based on the provided list of ScorerSupplier objects.
 */
@RequiredArgsConstructor
public class HybridScorerSupplier extends ScorerSupplier {

    private long cost = -1;
    @Getter
    private final List<ScorerSupplier> scorerSuppliers;
    private final HybridQueryWeight weight;
    private final ScoreMode scoreMode;
    private final LeafReaderContext context;

    @Override
    public Scorer get(long leadCost) throws IOException {
        List<Scorer> tScorers = new ArrayList<>();
        for (ScorerSupplier ss : scorerSuppliers) {
            if (Objects.nonNull(ss)) {
                tScorers.add(ss.get(leadCost));
            } else {
                tScorers.add(null);
            }
        }
        return new HybridQueryScorer(tScorers, scoreMode);
    }

    @Override
    public long cost() {
        if (cost == -1) {
            long cost = 0;
            for (ScorerSupplier ss : scorerSuppliers) {
                if (Objects.nonNull(ss)) {
                    cost += ss.cost();
                }
            }
            this.cost = cost;
        }
        return cost;
    }

    @Override
    public void setTopLevelScoringClause() throws IOException {
        for (ScorerSupplier ss : scorerSuppliers) {
            // sub scorers need to be able to skip too as calls to setMinCompetitiveScore get
            // propagated
            if (Objects.nonNull(ss)) {
                ss.setTopLevelScoringClause();
            }
        }
    }

    @Override
    public BulkScorer bulkScorer() throws IOException {
        List<Scorer> scorers = new ArrayList<>();
        for (Weight weight : weight.getWeights()) {
            Scorer scorer = weight.scorer(context);
            scorers.add(scorer);
        }
        return new HybridBulkScorer(scorers, scoreMode.needsScores(), context.reader().maxDoc());
    }
}
