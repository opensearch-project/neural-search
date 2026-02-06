/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.collapse;

import org.apache.lucene.util.BytesRef;

/**
 * Processes collapse operations on search results.
 * Determines the appropriate collapse strategy based on the field type and executes the collapse.
 */
public class CollapseExecutor {

    /**
     * Executes the collapse operation based on the provided collapse data.
     * Determines whether the collapse is for a keyword or numeric field,
     * creates the appropriate strategy, and performs the collapse.
     *
     * @param collapseDTO Data transfer object containing collapse configuration and results
     * @return The total number of documents that were collapsed during the operation
     */
    public int executeCollapse(CollapseDTO collapseDTO) {
        Class<?> collapseFieldType = collapseDTO.getCollapseFieldType();
        /**
         * Ideally, the fieldType should be derived from CollapseContext but currently there is no way of providing searchContext in phase results processor.
         * Through searchPhaseContext we can only get collapseBuilder which only contains the fieldName. To create collapseContext from builder we need QueryShardContext which is not present in phaseResultsProcessor.
         * As per current code, if the group value is null when the document does not have collapse field then we cannot determine the field type. Therefore, we always follow Numeric strategy in that case.
         * TODO: In future, when collapseContext is accessible then retrieve fieldType from there.
         *
         */
        CollapseStrategy collapseStrategy = collapseFieldType == BytesRef.class
            ? CollapseStrategy.createKeywordStrategy()
            : CollapseStrategy.createNumericStrategy();

        collapseStrategy.executeCollapse(collapseDTO);
        return collapseStrategy.getTotalCollapsedDocsCount();
    }
}
