/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.collapse;

import lombok.Getter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;

import java.util.Arrays;
import java.util.Map;

@Getter
public class CollapseResultUpdater {
    private int processedCollapsedDocsCount = 0;

    public <T> void updateCollapseResults(CollapseDTO collapseDTO) {
        Object[] objectCollapseValues = collapseDTO.getRelevantCollapseEntries().stream().map(Map.Entry::getKey).toArray(Object[]::new);

        ScoreDoc[] newCollapsedFieldDocs = collapseDTO.getRelevantCollapseEntries()
            .stream()
            .map(Map.Entry::getValue)
            .map(fieldDoc -> new FieldDoc(fieldDoc.doc, fieldDoc.score, Arrays.copyOfRange(fieldDoc.fields, 0, fieldDoc.fields.length - 1)))
            .toArray(ScoreDoc[]::new);

        TopDocsAndMaxScore updatedCollapseTopDocsAndMaxScore = new TopDocsAndMaxScore(
            new CollapseTopFieldDocs(
                collapseDTO.getCollapseField(),
                collapseDTO.getUpdatedCollapseTopDocs().getTotalHits(),
                newCollapsedFieldDocs,
                collapseDTO.getCollapseSort().getSort(),
                objectCollapseValues
            ),
            NormalizationProcessorWorkflow.maxScoreForShard(collapseDTO.getUpdatedCollapseTopDocs(), true)
        );

        if (collapseDTO.isCollapseFetchPhaseExecuted()) {
            collapseDTO.getCollapseQuerySearchResults()
                .get(collapseDTO.getCollapseShardIndex())
                .from(collapseDTO.getCollapseCombineScoresDTO().getFromValueForSingleShard());
        }

        collapseDTO.getCollapseQuerySearchResults()
            .get(collapseDTO.getCollapseShardIndex())
            .topDocs(
                updatedCollapseTopDocsAndMaxScore,
                collapseDTO.getCollapseQuerySearchResults().get(collapseDTO.getCollapseShardIndex()).sortValueFormats()
            );

        this.processedCollapsedDocsCount = newCollapsedFieldDocs.length;
    }

}
