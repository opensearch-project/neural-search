/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.Objects;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

/**
 * Turns the fused rescore guard's internal sentinel back into the score a user should see.
 *
 * <p>{@code FusedWindowGuardRescorer} separates the documents fusion ranked from the documents it did not by putting the
 * latter in a band below every ranked score — it has to be a score, because the score is the only channel a shard has to
 * the coordinator's merge, and {@code sort} is refused alongside {@code rescore} so there is no sort-value channel. The
 * band's floor is {@code -Float.MAX_VALUE}, which is correct for ordering and meaningless to a user: a document that
 * simply was not in the fused window would be reported at {@code -3.4028235E38}.
 *
 * <p>Without a rescore those same documents come back at exactly {@code 0.0} — they match only the non-scoring Tail — so
 * that is what the sentinel is mapped to here. The result is that a fused request with a rescore reports the same scores
 * for unranked documents as the same request without one, and the sentinel never leaves the coordinator.
 *
 * <p>Hit scores are mutated in place, the way {@code FusedExplanationMerger} mutates explanations: {@code SearchHit} has
 * a public score setter and the response passes its {@code SearchHits} through by reference. {@code SearchHits.maxScore}
 * is {@code final}, so the one case that needs a rebuild is a page whose top hit was demoted — which is exactly a shard,
 * or a request, that ranked nothing.
 */
final class FusedRescoreScoreNormalizer {

    private FusedRescoreScoreNormalizer() {}

    /**
     * @param response the response as the search phases built it
     * @param sentinel the score the guard demoted unranked documents to
     * @return the response with every sentinel score replaced by {@code 0.0}, or {@code response} itself when it carried
     *         none — a fused request whose rescore never had to demote anything is the common case and pays nothing
     */
    static SearchResponse normalize(final SearchResponse response, final float sentinel) {
        if (Objects.isNull(response) || Objects.isNull(response.getInternalResponse())) {
            return response;
        }
        SearchHits hits = response.getInternalResponse().hits();
        if (Objects.isNull(hits) || Objects.isNull(hits.getHits())) {
            return response;
        }
        for (SearchHit hit : hits.getHits()) {
            if (Float.compare(hit.getScore(), sentinel) == 0) {
                hit.score(0.0f);
            }
        }
        // The top hit was demoted, so the reported max score is the sentinel as well. That is the only field here that
        // cannot be corrected in place.
        if (Float.compare(hits.getMaxScore(), sentinel) == 0) {
            SearchHits corrected = new SearchHits(
                hits.getHits(),
                hits.getTotalHits(),
                0.0f,
                hits.getSortFields(),
                hits.getCollapseField(),
                hits.getCollapseValues()
            );
            return FusedResponseRebuilder.rebuild(response, null, response.isTimedOut(), corrected);
        }
        return response;
    }
}
