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
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;

import java.io.IOException;
import java.util.Objects;

/**
 * The abstract class for hybrid query leaf collector
 */
@Log4j2
public abstract class HybridLeafCollector implements LeafCollector {
    @Getter(AccessLevel.PACKAGE)
    HybridSubQueryScorer compoundQueryScorer;

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        if (scorer instanceof HybridSubQueryScorer) {
            compoundQueryScorer = (HybridSubQueryScorer) scorer;
        } else {
            compoundQueryScorer = getHybridQueryScorer(scorer);
            if (Objects.isNull(compoundQueryScorer)) {
                log.error("cannot find scorer of type HybridQueryScorer in a hierarchy of scorer {}", scorer);
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
        for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
            HybridSubQueryScorer hybridQueryScorer = getHybridQueryScorer(childScorable.child());
            if (Objects.nonNull(hybridQueryScorer)) {
                log.debug("found hybrid query scorer, it's child of scorer {}", childScorable.child().getClass().getSimpleName());
                return hybridQueryScorer;
            }
        }
        return null;
    }
}
