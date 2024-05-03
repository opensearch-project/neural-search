/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import org.apache.lucene.search.Query;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.ConcurrentQueryPhaseSearcher;
import org.opensearch.search.query.QueryCollectorContext;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Class that inherits ConcurrentQueryPhaseSearcher implementation but calls its search with only
 *  empty query collector context
 */
public class ConcurrentQueryPhaseSearcherWithEmptyQueryCollectorContext extends ConcurrentQueryPhaseSearcher {

    @Override
    protected boolean searchWithCollector(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        return searchWithCollector(
            searchContext,
            searcher,
            query,
            collectors,
            QueryCollectorContext.EMPTY_CONTEXT,
            hasFilterCollector,
            hasTimeout
        );
    }
}
