/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.opensearch.neuralsearch.query.HybridQueryScorer;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.search.profile.ProfilingWrapper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * The abstract class for hybrid query leaf collector
 */
@Log4j2
public abstract class HybridLeafCollector implements LeafCollector {
    @Getter(AccessLevel.PACKAGE)
    HybridSubQueryScorer compoundQueryScorer;

    // When profiling is enabled, HybridBulkScorer is bypassed and DefaultBulkScorer is used instead.
    // In that case we find HybridQueryScorer through the scorer hierarchy and use it to populate
    // per-sub-query scores on each collect() call.
    @Getter(AccessLevel.PACKAGE)
    HybridQueryScorer hybridQueryScorer;

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        if (scorer instanceof HybridSubQueryScorer) {
            // Normal path: HybridBulkScorer sets HybridSubQueryScorer directly
            compoundQueryScorer = (HybridSubQueryScorer) scorer;
        } else {
            // Try to find HybridSubQueryScorer in the hierarchy (existing behavior)
            compoundQueryScorer = findScorerInHierarchy(scorer, HybridSubQueryScorer.class);

            if (Objects.isNull(compoundQueryScorer)) {
                // Profiler path: DefaultBulkScorer is used, so we look for HybridQueryScorer instead.
                // When found, we create a HybridSubQueryScorer adapter to bridge the two paths.
                hybridQueryScorer = findHybridQueryScorer(scorer);
                if (Objects.nonNull(hybridQueryScorer)) {
                    int numSubQueries = hybridQueryScorer.getSubScorers().size();
                    compoundQueryScorer = new HybridSubQueryScorer(numSubQueries);
                    log.debug(
                        "profiler mode detected: created HybridSubQueryScorer adapter from HybridQueryScorer with {} sub-queries",
                        numSubQueries
                    );
                } else {
                    log.error("cannot find scorer of type HybridSubQueryScorer in a hierarchy of scorer {}", scorer);
                }
            }
        }
    }

    /**
     * When in profiler mode (HybridQueryScorer path), populate the HybridSubQueryScorer's score array
     * from the HybridQueryScorer's current sub-matches. This must be called before reading scores
     * in collect().
     */
    void populateScoresFromHybridQueryScorer() throws IOException {
        if (Objects.isNull(hybridQueryScorer) || Objects.isNull(compoundQueryScorer)) {
            return;
        }
        compoundQueryScorer.resetScores();
        float[] subQueryScores = compoundQueryScorer.getSubQueryScores();
        int currentDoc = hybridQueryScorer.docID();
        List<Scorer> subScorers = hybridQueryScorer.getSubScorers();
        for (int i = 0; i < subScorers.size(); i++) {
            Scorer subScorer = subScorers.get(i);
            if (Objects.nonNull(subScorer) && subScorer.docID() == currentDoc && currentDoc != DocIdSetIterator.NO_MORE_DOCS) {
                subQueryScores[i] = subScorer.score();
            }
        }
    }

    /**
     * Finds HybridQueryScorer in the scorer hierarchy. Uses ProfilingWrapper interface to unwrap
     * profiling wrappers (like ProfileScorer) without reflection.
     */
    @SuppressWarnings("unchecked")
    private HybridQueryScorer findHybridQueryScorer(final Scorable scorer) throws IOException {
        if (Objects.isNull(scorer)) {
            return null;
        }
        if (scorer instanceof HybridQueryScorer) {
            return (HybridQueryScorer) scorer;
        }
        // when profiling is enabled, ProfileScorer implements ProfilingWrapper
        if (scorer instanceof ProfilingWrapper) {
            Scorer wrappedScorer = ((ProfilingWrapper<Scorer>) scorer).getDelegate();
            if (Objects.nonNull(wrappedScorer)) {
                return findHybridQueryScorer(wrappedScorer);
            }
        }
        // Also check children for other wrapper types
        for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
            HybridQueryScorer found = findHybridQueryScorer(childScorable.child());
            if (Objects.nonNull(found)) {
                return found;
            }
        }
        return null;
    }

    /**
     * Traverses the scorer tree via getChildren() to find a scorer of the specified type.
     */
    @SuppressWarnings("unchecked")
    private <T extends Scorable> T findScorerInHierarchy(final Scorable scorer, final Class<T> targetType) throws IOException {
        if (Objects.isNull(scorer)) {
            return null;
        }
        if (targetType.isInstance(scorer)) {
            return (T) scorer;
        }
        for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
            T found = findScorerInHierarchy(childScorable.child(), targetType);
            if (Objects.nonNull(found)) {
                return found;
            }
        }
        return null;
    }
}
