/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.collapse;

import lombok.Getter;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;

/**
 * Processes collapse operations on search results.
 * Determines the appropriate collapse strategy based on the field type and executes the collapse.
 */
@Getter
public class CollapseExecutor {
    private int totalCollapsedDocsCount = 0;

    /**
     * Executes the collapse operation based on the provided collapse data.
     * Determines whether the collapse is for a keyword or numeric field,
     * creates the appropriate strategy, and performs the collapse.
     *
     * @param collapseDTO Data transfer object containing collapse configuration and results
     */
    public void executeCollapse(CollapseDTO collapseDTO) {
        boolean isKeywordCollapse = isCollapseOnKeywordField(collapseDTO);
        CollapseStrategy collapseStrategy = isKeywordCollapse
            ? CollapseStrategy.createKeywordStrategy()
            : CollapseStrategy.createNumericStrategy();

        collapseStrategy.executeCollapse(collapseDTO);
        this.totalCollapsedDocsCount = collapseStrategy.getTotalCollapsedDocsCount();
    }

    private boolean isCollapseOnKeywordField(CollapseDTO collapseDTO) {
        return ((CollapseTopFieldDocs) collapseDTO.getCollapseQueryTopDocs()
            .get(collapseDTO.getIndexOfFirstNonEmpty())
            .getTopDocs()
            .getFirst()).collapseValues[0] instanceof BytesRef;
    }
}
