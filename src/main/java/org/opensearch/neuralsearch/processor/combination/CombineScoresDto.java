/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.search.query.QuerySearchResult;

/**
 * DTO object to hold data required for Score Combination.
 */
@AllArgsConstructor
@Builder
@Getter
public class CombineScoresDto {
    @NonNull
    private List<CompoundTopDocs> queryTopDocs;
    @NonNull
    private ScoreCombinationTechnique scoreCombinationTechnique;
    @NonNull
    private List<QuerySearchResult> querySearchResults;
    @Nullable
    private Sort sort;
    private int fromValueForSingleShard;
    private boolean isSingleShard;
    private Float minScore;
    private boolean isCollapseEnabled;
    /**
     * The request's {@code search_after} cursor for a {@code _score}-led sort, one per element of {@link #queryTopDocs}, each
     * typed with that shard's own sort fields and formats — the cursor compares against the combined score, so it can only be
     * applied here, after combination. {@code null} otherwise: a field-sorted cursor is not applied here, and a request without
     * {@code search_after} has nothing to apply. Core never sends a {@code _score}-led cursor with collapse: it accepts
     * {@code search_after} with collapse only when the sort is the collapse field alone.
     */
    @Nullable
    private List<FieldDoc> searchAfterPerShard;
}
