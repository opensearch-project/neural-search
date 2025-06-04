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
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflowUtil;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Updates collapse results based on the processed collapse data.
 * This class is responsible for creating new collapsed field documents and updating the search results.
 */
@Getter
public class CollapseResultUpdater {
    private AtomicInteger processedCollapsedDocsCount = new AtomicInteger(0);

    public void updateCollapseResults(CollapseDTO collapseDTO) {
        if (!isValidCollapseData(collapseDTO)) {
            return;
        }

        ScoreDoc[] newCollapsedFieldDocs = createCollapsedFieldDocs(collapseDTO);
        TopDocsAndMaxScore updatedTopDocs = createUpdatedTopDocs(collapseDTO, newCollapsedFieldDocs);
        updateSearchResults(collapseDTO, updatedTopDocs);

        this.processedCollapsedDocsCount.set(newCollapsedFieldDocs.length);
    }

    private boolean isValidCollapseData(CollapseDTO collapseDTO) {
        return collapseDTO != null && collapseDTO.getRelevantCollapseEntries() != null;
    }

    private ScoreDoc[] createCollapsedFieldDocs(CollapseDTO collapseDTO) {
        return collapseDTO.getRelevantCollapseEntries().stream().map(Map.Entry::getValue).map(fieldDoc -> {
            if (isInvalidFieldDoc(fieldDoc)) {
                return new FieldDoc(fieldDoc.doc, fieldDoc.score, new Object[0]);
            }
            int newLength = fieldDoc.fields.length - 1;
            Object[] newFields = new Object[newLength];
            System.arraycopy(fieldDoc.fields, 0, newFields, 0, newLength);
            return new FieldDoc(fieldDoc.doc, fieldDoc.score, newFields);
        }).toArray(ScoreDoc[]::new);
    }

    private static boolean isInvalidFieldDoc(FieldDoc fieldDoc) {
        return fieldDoc.fields == null || fieldDoc.fields.length <= 1;
    }

    private TopDocsAndMaxScore createUpdatedTopDocs(CollapseDTO collapseDTO, ScoreDoc[] newCollapsedFieldDocs) {
        Object[] objectCollapseValues = collapseDTO.getRelevantCollapseEntries().stream().map(Map.Entry::getKey).toArray(Object[]::new);

        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            collapseDTO.getCollapseField(),
            collapseDTO.getUpdatedCollapseTopDocs().getTotalHits(),
            newCollapsedFieldDocs,
            collapseDTO.getCollapseSort().getSort(),
            objectCollapseValues
        );

        return new TopDocsAndMaxScore(
            collapseTopFieldDocs,
            NormalizationProcessorWorkflowUtil.maxScoreForShard(collapseDTO.getUpdatedCollapseTopDocs(), true)
        );
    }

    private void updateSearchResults(CollapseDTO collapseDTO, TopDocsAndMaxScore updatedTopDocs) {
        var searchResults = collapseDTO.getCollapseQuerySearchResults().get(collapseDTO.getCollapseShardIndex());

        if (collapseDTO.isFetchPhaseExecuted()) {
            searchResults.from(collapseDTO.getCollapseCombineScoresDTO().getFromValueForSingleShard());
        }

        searchResults.topDocs(updatedTopDocs, searchResults.sortValueFormats());
    }
}
