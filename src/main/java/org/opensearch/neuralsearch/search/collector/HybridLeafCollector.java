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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

    // ProfileScorer is package-private in OpenSearch core, so we identify it by class name
    private static final String PROFILE_SCORER_CLASS_NAME = "org.opensearch.search.profile.query.ProfileScorer";

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        if (scorer instanceof HybridSubQueryScorer) {
            // Normal path: HybridBulkScorer sets HybridSubQueryScorer directly
            compoundQueryScorer = (HybridSubQueryScorer) scorer;
        } else {
            // Try to find HybridSubQueryScorer in the hierarchy (existing behavior)
            compoundQueryScorer = getHybridSubQueryScorer(scorer);

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

    private HybridSubQueryScorer getHybridSubQueryScorer(final Scorable scorer) throws IOException {
        if (Objects.isNull(scorer)) {
            return null;
        }
        if (scorer instanceof HybridSubQueryScorer) {
            return (HybridSubQueryScorer) scorer;
        }

        for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
            HybridSubQueryScorer hybridQueryScorer = getHybridSubQueryScorer(childScorable.child());
            if (Objects.nonNull(hybridQueryScorer)) {
                return hybridQueryScorer;
            }
        }
        return null;
    }

    /**
     * Finds HybridQueryScorer in the scorer hierarchy. When profiling is enabled,
     * the hierarchy is ProfileScorer -> HybridQueryScorer.
     */
    private HybridQueryScorer findHybridQueryScorer(final Scorable scorer) throws IOException {
        if (Objects.isNull(scorer)) {
            return null;
        }
        if (scorer instanceof HybridQueryScorer) {
            return (HybridQueryScorer) scorer;
        }

        // When profiling is enabled, ProfileScorer wraps HybridQueryScorer.
        // ProfileScorer doesn't expose children via getChildren() so we need reflection.
        if (isProfileScorer(scorer)) {
            Scorer wrappedScorer = getWrappedScorerFromProfileScorer(scorer);
            if (Objects.nonNull(wrappedScorer)) {
                return findHybridQueryScorer(wrappedScorer);
            }
        }
        return null;
    }

    /**
     * Checks if the given scorer is a ProfileScorer by comparing class name.
     * We use class name comparison because ProfileScorer is package-private in OpenSearch core.
     */
    private boolean isProfileScorer(Scorable scorer) {
        return Objects.nonNull(scorer) && PROFILE_SCORER_CLASS_NAME.equals(scorer.getClass().getName());
    }

    /**
     * Extracts the wrapped scorer from a ProfileScorer using reflection.
     * ProfileScorer is package-private in OpenSearch core, so we use reflection to call
     * its public getWrappedScorer() method which is available since OpenSearch 3.6.0.
     */
    private Scorer getWrappedScorerFromProfileScorer(Scorable profileScorer) {
        try {
            Method getWrappedScorerMethod = profileScorer.getClass().getMethod("getWrappedScorer");
            Object result = getWrappedScorerMethod.invoke(profileScorer);
            if (result instanceof Scorer) {
                return (Scorer) result;
            }
        } catch (NoSuchMethodException e) {
            log.error("ProfileScorer doesn't have getWrappedScorer method, profiling with hybrid query requires OpenSearch 3.6.0+");
        } catch (IllegalAccessException | InvocationTargetException e) {
            log.error("Failed to invoke getWrappedScorer on ProfileScorer: {}", e.getMessage());
        }
        return null;
    }
}
