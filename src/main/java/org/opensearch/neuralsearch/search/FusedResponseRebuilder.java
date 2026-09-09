/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.Map;
import java.util.Objects;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;

import lombok.NoArgsConstructor;

/**
 * The one place a fused ({@code fusion}) hybrid's coordinator-side reporting rebuilds a search response.
 *
 * <p>Neither of the things fused mode reports is writable on a response in place. The profile section is replaced
 * wholesale — {@link SearchProfileShardResults} takes its map whole and never mutates it — and {@code timed_out} is a
 * constructor argument on {@link InternalSearchResponse} with no setter. Both therefore mean constructing a new response
 * around the sections that are being kept, which is what core itself does on its single-remote-cluster CCS path in
 * {@code TransportSearchAction} and in {@code SearchResponseMerger}.
 *
 * <p><b>This class exists so that there is exactly one such argument list to keep correct.</b> A field left off it would
 * be silently dropped from a fused response that reported something while an otherwise identical response that reported
 * nothing kept it — a difference no assertion in either merger would catch, because neither merger looks at the fields it
 * is only passing through. So both calls below use the widest constructor core offers, every field is asserted in
 * {@code FusedResponseRebuilderTests}, and a canary there fails when core widens either constructor: core widens by
 * adding a constructor and keeping the old ones, so a new field arrives as a wider constructor rather than as a compile
 * error here.
 *
 * <p>Callers compose their overrides into a single call rather than chaining rebuilds, so a response is rebuilt at most
 * once per request no matter how many things it reports.
 */
@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class FusedResponseRebuilder {

    /**
     * As {@link #rebuild(SearchResponse, SearchProfileShardResults, boolean)}, additionally substituting the response's
     * {@link SearchHits}. Needed because {@code SearchHits.maxScore} is {@code final}: everything else about a hit can be
     * corrected in place, but a page whose reported max score has to change can only be replaced.
     *
     * @param hits the hits to substitute, or {@code null} to keep the response's own
     */
    public static SearchResponse rebuild(
        final SearchResponse response,
        final SearchProfileShardResults profileResults,
        final boolean timedOut,
        final SearchHits hits
    ) {
        SearchResponseSections sections = response.getInternalResponse();
        if (Objects.isNull(profileResults) && Objects.isNull(hits) && timedOut == sections.timedOut()) {
            return response;
        }
        InternalSearchResponse rebuilt = new InternalSearchResponse(
            Objects.nonNull(hits) ? hits : sections.hits(),
            (InternalAggregations) sections.aggregations(),
            sections.suggest(),
            Objects.nonNull(profileResults) ? profileResults : ownProfileResults(sections),
            timedOut,
            sections.terminatedEarly(),
            sections.getNumReducePhases(),
            sections.getSearchExtBuilders(),
            sections.getProcessorResult()
        );
        return new SearchResponse(
            rebuilt,
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getPhaseTook(),
            response.getShardFailures(),
            response.getClusters(),
            response.pointInTimeId()
        );
    }

    /**
     * {@code response} with the given overrides applied, or {@code response} itself when neither changes anything.
     *
     * <p>Delegates to the four-argument form with no hits override, so that the argument list this class exists to keep
     * correct is written down exactly once.
     *
     * @param response       the response as the search phases built it
     * @param profileResults the profile section to substitute, or {@code null} to keep the response's own
     * @param timedOut       the value {@code timed_out} should carry — callers pass the OR of the response's own flag and
     *                       whatever they are contributing, never a bare {@code false} that would clear it
     */
    public static SearchResponse rebuild(
        final SearchResponse response,
        final SearchProfileShardResults profileResults,
        final boolean timedOut
    ) {
        return rebuild(response, profileResults, timedOut, null);
    }

    /**
     * The response's existing profile section, for a rebuild that is not replacing it.
     *
     * <p>Reconstructed from {@link SearchResponseSections#profile()} because the section itself is {@code protected} in
     * core and unreachable from here. The round-trip is lossless for every section that carries entries, which is what
     * makes it safe here: it maps a populated map back to an equivalent section, and answers {@code null} for a section
     * that is absent or empty, which {@link #rebuild} then passes through as the response's own.
     *
     * <p>The one shape it cannot preserve is a section that is <i>present but holds no entries</i> — that renders as an
     * empty {@code profile} block and comes back as no block at all. Reaching this method with such a section takes a
     * profiled search that produced no shard entries, and reaching it at all requires the caller to have substituted no
     * profile section while still changing something, i.e. a rebuild driven by {@code timed_out} alone. Note that "the
     * caller substituted nothing" is <b>not</b> the same as "the request was not profiled": a search request processor
     * runs after this plugin's {@code ActionFilter} has already decided whether to attach a profile merger, so a request
     * this plugin saw as unprofiled can still be profiled by the time core builds the response.
     */
    private static SearchProfileShardResults ownProfileResults(final SearchResponseSections sections) {
        Map<String, ProfileShardResult> own = sections.profile();
        return Objects.isNull(own) || own.isEmpty() ? null : new SearchProfileShardResults(own);
    }
}
