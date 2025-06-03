/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.collapse;

import lombok.Getter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public abstract class CollapseStrategy {
    protected int totalCollapsedDocsCount = 0;

    public abstract void executeCollapse(CollapseDTO collapseDTO);

    private static class KeywordCollapseStrategy extends CollapseStrategy {
        @Override
        public void executeCollapse(CollapseDTO collapseDTO) {
            CollapseDataCollector<BytesRef> collapseDataCollector = new CollapseDataCollector<>(collapseDTO);

            collapseDataCollector.collectCollapseData(collapseDTO);
            List<Map.Entry<BytesRef, FieldDoc>> sortedCollapseEntries = collapseDataCollector.getSortedCollapseEntries();

            Map<Integer, List<Map.Entry<BytesRef, FieldDoc>>> shardToCollapseEntriesMap = sortedCollapseEntries.stream()
                .collect(Collectors.groupingBy(entry -> collapseDataCollector.getCollapseShardIndex(entry.getKey())));

            CollapseResultUpdater collapseResultUpdater = new CollapseResultUpdater();

            for (int shardIndex = 0; shardIndex < collapseDTO.getCollapseQuerySearchResults().size(); shardIndex++) {
                CompoundTopDocs updatedCollapseTopDocs = collapseDTO.getCollapseQueryTopDocs().get(shardIndex);
                List<Map.Entry<BytesRef, FieldDoc>> relevantCollapseEntries = shardToCollapseEntriesMap.getOrDefault(
                    shardIndex,
                    Collections.emptyList()
                );

                collapseDTO.updateForShard(
                    relevantCollapseEntries,
                    collapseDataCollector.getCollapseField(),
                    updatedCollapseTopDocs,
                    shardIndex
                );

                collapseResultUpdater.updateCollapseResults(collapseDTO);
                this.totalCollapsedDocsCount += collapseResultUpdater.getProcessedCollapsedDocsCount();
            }
        }
    }

    private static class NumericCollapseStrategy extends CollapseStrategy {
        @Override
        public void executeCollapse(CollapseDTO collapseDTO) {
            CollapseDataCollector<Long> collapseDataCollector = new CollapseDataCollector<>(collapseDTO);

            collapseDataCollector.collectCollapseData(collapseDTO);
            List<Map.Entry<Long, FieldDoc>> sortedCollapseEntries = collapseDataCollector.getSortedCollapseEntries();

            Map<Integer, List<Map.Entry<Long, FieldDoc>>> shardToCollapseEntriesMap = sortedCollapseEntries.stream()
                .collect(Collectors.groupingBy(entry -> collapseDataCollector.getCollapseShardIndex(entry.getKey())));

            CollapseResultUpdater collapseResultUpdater = new CollapseResultUpdater();

            for (int shardIndex = 0; shardIndex < collapseDTO.getCollapseQuerySearchResults().size(); shardIndex++) {
                CompoundTopDocs updatedCollapseTopDocs = collapseDTO.getCollapseQueryTopDocs().get(shardIndex);
                List<Map.Entry<Long, FieldDoc>> relevantCollapseEntries = shardToCollapseEntriesMap.getOrDefault(
                    shardIndex,
                    Collections.emptyList()
                );

                collapseDTO.updateForShard(
                    relevantCollapseEntries,
                    collapseDataCollector.getCollapseField(),
                    updatedCollapseTopDocs,
                    shardIndex
                );

                collapseResultUpdater.updateCollapseResults(collapseDTO);
                this.totalCollapsedDocsCount += collapseResultUpdater.getProcessedCollapsedDocsCount();
            }
        }
    }

    public static CollapseStrategy createKeywordStrategy() {
        return new KeywordCollapseStrategy();
    }

    public static CollapseStrategy createNumericStrategy() {
        return new NumericCollapseStrategy();
    }
}
