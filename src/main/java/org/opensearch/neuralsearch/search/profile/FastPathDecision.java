/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.profile;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Whether one fused ({@code fusion}) hybrid takes the fast path — its page assembled on the coordinator from the legs'
 * hits, no second round — and, when it does not, the first reason that stopped it. Rendered under the coordinator's
 * profile entry as {@code debug.fast_path}, so a user can read off the same request why it ran one round or two.
 *
 * <p>A profiled request never takes the fast path itself: round 2's query tree is part of what a profile reports, so
 * {@code profile: true} is one of the shapes that keep two rounds. What this reports for a profiled request is the
 * decision the <b>same request without {@code profile}</b> gets on this coordinator — not a simulation: every condition
 * before the legs run (the request's shape, its legs, the resolved pipeline, the fetch volume from this coordinator's
 * observed {@code _source} sizes) is evaluated exactly as the unprofiled request evaluates it, and the conditions after
 * the legs run ({@link #countSettled}, the page fitting the ranked window) are read off the legs' actual answers, which
 * are the same under profile. The one thing that is per coordinator — which {@code _source} sizes it has observed — is
 * also what the unprofiled request would have met on the coordinator that served this one.
 *
 * <p>{@link #refusedBy} is {@code null} while nothing has refused; the reasons are checked in the order the fast path
 * checks them, and the first to fail is the one reported. Optional facts are set when they were evaluated:
 * {@link #fetchEstimateBytes}/{@link #fetchBudgetBytes} once the fetch volume was weighed, {@link #countSettled} once
 * the legs have answered.
 */
@Getter
@Setter
@Accessors(chain = true, fluent = true)
public final class FastPathDecision {

    /** The fused hybrid is not the request's own top-level query; only that one can have its page assembled. */
    public static final String NESTED_HYBRID = "nested_hybrid";
    /** A request feature that needs round 2 over the fused ranking; {@link #detail} names it. */
    public static final String REQUEST_SHAPE = "request_shape";
    /** {@code track_total_hits: true} — an exact count needs the Tail. */
    public static final String EXACT_TOTALS = "exact_totals";
    /** A leg carries {@code _name}; the two-round path registers it against every returned document. */
    public static final String LEG_NAME = "leg_name";
    /** A leg declares {@code inner_hits}; the two-round path computes them for every returned document. */
    public static final String LEG_INNER_HITS = "leg_inner_hits";
    /** The resolved search pipeline has response processors, which would run before the assembled page reaches them. */
    public static final String RESPONSE_PROCESSORS = "response_processors";
    /** No response of this request's {@code _source} filter shape has been observed yet on this coordinator. */
    public static final String SOURCE_SIZE_UNOBSERVED = "source_size_unobserved";
    /** The request's indices or their mappings could not be read on the coordinator; the fetch volume is unknown. */
    public static final String FETCH_VOLUME_UNKNOWN = "fetch_volume_unknown";
    /** The legs would fetch more beyond the page than {@link #fetchBudgetBytes} allows. */
    public static final String FETCH_BUDGET = "fetch_budget";
    /** The legs returned nothing to fuse; round 2 is a {@code match_none} either way. */
    public static final String NO_CANDIDATES = "no_candidates";
    /** The requested page reaches past the ranked window, where only the Tail has documents. */
    public static final String PAGE_BEYOND_WINDOW = "page_beyond_window";
    /** The request wants a count beyond the window and no leg proved it. */
    public static final String COUNT_NOT_SETTLED = "count_not_settled";

    private String refusedBy;
    private String detail;
    private Long fetchEstimateBytes;
    private Long fetchBudgetBytes;
    private Boolean countSettled;

    /** Nothing has refused yet — the verdict so far is the fast path. */
    public boolean allowsSoFar() {
        return Objects.isNull(refusedBy);
    }

    /** Record the first refusal; later calls do not overwrite it, so the reported reason is the first one met. */
    public FastPathDecision refuse(final String reason, final String reasonDetail) {
        if (Objects.isNull(refusedBy)) {
            refusedBy = reason;
            detail = reasonDetail;
        }
        return this;
    }

    /** The rendering under the coordinator entry's {@code debug}: {@code would_take}, then only what applies. */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("would_take", allowsSoFar());
        if (Objects.nonNull(refusedBy)) {
            map.put("refused_by", refusedBy);
            if (Objects.nonNull(detail)) {
                map.put("detail", detail);
            }
        }
        if (Objects.nonNull(fetchEstimateBytes)) {
            map.put("fetch_estimate_bytes", fetchEstimateBytes);
        }
        if (Objects.nonNull(fetchBudgetBytes)) {
            map.put("fetch_budget_bytes", fetchBudgetBytes);
        }
        if (Objects.nonNull(countSettled)) {
            map.put("count_settled", countSettled);
        }
        return map;
    }
}
