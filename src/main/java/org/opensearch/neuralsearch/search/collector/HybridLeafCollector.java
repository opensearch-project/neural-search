/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * The abstract class for hybrid query leaf collector
 */
@Log4j2
public abstract class HybridLeafCollector implements LeafCollector {
    @Getter(AccessLevel.PACKAGE)
    HybridSubQueryScorer compoundQueryScorer;

    // ProfileScorer is package-private in OpenSearch core, so we identify it by class name
    private static final String PROFILE_SCORER_CLASS_NAME = "org.opensearch.search.profile.query.ProfileScorer";

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        if (scorer instanceof HybridSubQueryScorer) {
            compoundQueryScorer = (HybridSubQueryScorer) scorer;
        } else {
            compoundQueryScorer = getHybridQueryScorer(scorer);
            if (Objects.isNull(compoundQueryScorer)) {
                log.error("cannot find scorer of type HybridSubQueryScorer in a hierarchy of scorer {}", scorer);
            }
        }
    }

    private HybridSubQueryScorer getHybridQueryScorer(final Scorable scorer) throws IOException {
        if (Objects.isNull(scorer)) {
            return null;
        }
        if (scorer instanceof HybridSubQueryScorer) {
            return (HybridSubQueryScorer) scorer;
        }

        // When profiling is enabled, OpenSearch wraps scorers in ProfileScorer.
        // ProfileScorer doesn't expose the wrapped scorer via getChildren(), so we need
        // to use getWrappedScorer() method to access the underlying scorer.
        // Since ProfileScorer is package-private, we use reflection to access it.
        if (isProfileScorer(scorer)) {
            Scorer wrappedScorer = getWrappedScorerFromProfileScorer(scorer);
            if (Objects.nonNull(wrappedScorer)) {
                log.debug("unwrapping ProfileScorer to access underlying scorer: {}", wrappedScorer.getClass().getSimpleName());
                return getHybridQueryScorer(wrappedScorer);
            }
        }

        for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
            HybridSubQueryScorer hybridQueryScorer = getHybridQueryScorer(childScorable.child());
            if (Objects.nonNull(hybridQueryScorer)) {
                log.debug("found hybrid query scorer, it's child of scorer {}", childScorable.child().getClass().getSimpleName());
                return hybridQueryScorer;
            }
        }
        return null;
    }

    /**
     * Checks if the given scorer is a ProfileScorer by comparing class name.
     * We use class name comparison because ProfileScorer is package-private in OpenSearch core.
     *
     * @param scorer the scorer to check
     * @return true if the scorer is a ProfileScorer
     */
    private boolean isProfileScorer(Scorable scorer) {
        return Objects.nonNull(scorer) && PROFILE_SCORER_CLASS_NAME.equals(scorer.getClass().getName());
    }

    /**
     * Extracts the wrapped scorer from a ProfileScorer using reflection.
     * ProfileScorer has a getWrappedScorer() method that returns the underlying scorer.
     *
     * @param profileScorer the ProfileScorer to unwrap
     * @return the wrapped Scorer, or null if unable to extract
     */
    private Scorer getWrappedScorerFromProfileScorer(Scorable profileScorer) {
        try {
            Method getWrappedScorerMethod = profileScorer.getClass().getMethod("getWrappedScorer");
            Object result = getWrappedScorerMethod.invoke(profileScorer);
            if (result instanceof Scorer) {
                return (Scorer) result;
            }
        } catch (NoSuchMethodException e) {
            log.debug("ProfileScorer doesn't have getWrappedScorer method - OpenSearch version may not support this");
        } catch (IllegalAccessException | InvocationTargetException e) {
            log.debug("Failed to invoke getWrappedScorer on ProfileScorer: {}", e.getMessage());
        }
        return null;
    }
}
