/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import lombok.Getter;
import org.apache.lucene.search.SortField;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;

import java.util.List;

/**
 * Class that holds all parameters required for processing collector results in HybridCollectorResultsUtil
 */
@Getter
public class HybridCollectorResultsUtilParams {
    private final SearchContext searchContext;
    private final TopDocsMerger topDocsMerger;
    private final int trackTotalHitsUpTo;
    private final boolean isSortEnabled;
    private final boolean isCollapseEnabled;
    private SortField[] sortFields = null;
    private final List<RescoreContext> rescoreContexts;
    private SortAndFormats sortAndFormats = null;
    private DocValueFormat[] docValueFormats = null;

    private HybridCollectorResultsUtilParams(Builder builder) {
        this.searchContext = builder.searchContext;
        this.topDocsMerger = new TopDocsMerger(searchContext.sort(), searchContext.collapse());
        this.trackTotalHitsUpTo = searchContext.trackTotalHitsUpTo();
        this.isSortEnabled = searchContext.sort() != null;
        if (isSortEnabled) {
            setSortEnabledParameters();
        }
        this.isCollapseEnabled = searchContext.collapse() != null;
        if (isCollapseEnabled && isSortEnabled == false) {
            this.docValueFormats = new DocValueFormat[] { DocValueFormat.RAW };
        }
        this.rescoreContexts = searchContext.rescore();
    }

    private void setSortEnabledParameters() {
        this.sortAndFormats = searchContext.sort();
        this.sortFields = sortAndFormats.sort.getSort();
        this.docValueFormats = sortAndFormats.formats;
    }

    /**
     * Builder class
     */
    public static class Builder {
        private SearchContext searchContext;

        public Builder searchContext(SearchContext searchContext) {
            this.searchContext = searchContext;
            return this;
        }

        public HybridCollectorResultsUtilParams build() {
            return new HybridCollectorResultsUtilParams(this);
        }
    }
}
