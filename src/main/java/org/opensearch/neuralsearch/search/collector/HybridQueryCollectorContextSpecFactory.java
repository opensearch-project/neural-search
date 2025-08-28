/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.Query;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorArguments;
import org.opensearch.search.query.QueryCollectorContextSpec;
import org.opensearch.search.query.QueryCollectorContextSpecFactory;

import java.util.Optional;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQuery;

/**
 * Factory class for HybridQueryCollectorContextSpec. In case of hybrid query, it will create the spec which will retrieved in the QueryPhase.
 */
@Log4j2
public class HybridQueryCollectorContextSpecFactory implements QueryCollectorContextSpecFactory {

    @Override
    public Optional<QueryCollectorContextSpec> createQueryCollectorContextSpec(
        SearchContext searchContext,
        Query query,
        QueryCollectorArguments queryCollectorArguments
    ) {
        if (isHybridQuery(query, searchContext)) {
            return Optional.of(new HybridQueryCollectorContextSpec(searchContext));
        }
        return Optional.empty();
    }
}
