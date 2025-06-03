/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.collapse;

import lombok.Getter;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;

@Getter
public class CollapseProcessor {
    private int totalCollapsedDocsCount = 0;

    public void executeCollapse(CollapseDTO collapseDTO) {
        boolean isKeywordCollapse = isKeywordCollapseType(collapseDTO);
        CollapseStrategy collapseStrategy = isKeywordCollapse
            ? CollapseStrategy.createKeywordStrategy()
            : CollapseStrategy.createNumericStrategy();

        collapseStrategy.executeCollapse(collapseDTO);
        this.totalCollapsedDocsCount = collapseStrategy.getTotalCollapsedDocsCount();
    }

    private boolean isKeywordCollapseType(CollapseDTO collapseDTO) {
        return ((CollapseTopFieldDocs) collapseDTO.getCollapseQueryTopDocs()
            .get(collapseDTO.getIndexOfFirstNonEmpty())
            .getTopDocs()
            .getFirst()).collapseValues[0] instanceof BytesRef;
    }
}
