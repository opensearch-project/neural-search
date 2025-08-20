/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.apache.lucene.search.Query;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorArguments;
import org.opensearch.search.query.QueryCollectorContextSpec;
import org.opensearch.search.query.QueryCollectorContextSpecFactory;

import java.io.IOException;
import java.util.Optional;

import static org.opensearch.neuralsearch.util.HybridQueryUtil.extractHybridQuery;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQuery;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.isWrappedHybridQuery;
import static org.opensearch.neuralsearch.util.HybridQueryUtil.validateHybridQuery;

public class HybridQueryCollectorContextSpecFactory implements QueryCollectorContextSpecFactory {

    @Override
    public Optional<QueryCollectorContextSpec> createQueryCollectorContextSpec(
            SearchContext searchContext,
            QueryCollectorArguments queryCollectorArguments
    ) throws IOException {
        if (isHybridQuery(searchContext.query(), searchContext)) {
            Query query = extractHybridQuery(searchContext);
            validateHybridQuery((HybridQuery) query);
            searchContext.parsedQuery(new ParsedQuery(query));
            return Optional.of(new HybridQueryCollectorContextSpec(searchContext));
        } else if (isWrappedHybridQuery(searchContext.query())) {
            throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
        }
        return Optional.empty();
    }
}