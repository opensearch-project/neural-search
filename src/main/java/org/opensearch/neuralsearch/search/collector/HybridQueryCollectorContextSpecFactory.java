/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorArguments;
import org.opensearch.search.query.QueryCollectorContextSpec;
import org.opensearch.search.query.QueryCollectorContextSpecFactory;

import java.io.IOException;
import java.util.Optional;

import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQuery;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQueryWrappedInBooleanQuery;

public class HybridQueryCollectorContextSpecFactory implements QueryCollectorContextSpecFactory {

    @Override
    public Optional<QueryCollectorContextSpec> createQueryCollectorContextSpec(
        SearchContext searchContext,
        QueryCollectorArguments queryCollectorArguments
    ) throws IOException {
        if (isHybridQuery(searchContext.query(), searchContext)
            || isHybridQueryWrappedInBooleanQuery(searchContext, searchContext.query())) {
            return Optional.of(new HybridQueryCollectorContextSpec(searchContext));
        }
        return Optional.empty();
    }
}
