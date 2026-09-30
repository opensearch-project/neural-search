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
 * observed {@code _source} sizes) is evaluated exactly as the unprofiled request evaluates it, and the conditions after the
 * legs run (the page fitting the ranked window, and {@link #countSettled} except where only the count round could have
 * settled it — see below) are read off the legs' actual answers, which are the same under profile. The one thing that is per
 * coordinator — which {@code _source} sizes it has observed — is also what the unprofiled request would have met on the
 * coordinator that served this one.
 *
 * <p>One precondition is the exception: whether this hybrid is the request's own query. A profiled request is marked as
 * the root whatever its shape, while an unprofiled one is marked by the hits consumer the filter attaches only when the
 * shape already allows the fast path — so a profiled request that a shape feature refuses reports that feature's own
 * refusal ({@link #REQUEST_SHAPE}, or {@link #EXACT_TOTALS} for {@code track_total_hits: true}), where its unprofiled
 * twin would have stopped one check earlier at {@link #NESTED_HYBRID}. For the request as submitted the verdict is the
 * same either way and only the reason differs, the reported one being the more specific.
 *
 * <p>One way the two can differ on {@code would_take} is the one thing neither reads at the same moment. The twin's gate is
 * two shape reads — the filter's, of the request as submitted, and the rewrite's, after the search pipeline's request
 * processors have run — because the consumer the filter attaches is also what arms the path. This report is the rewrite's
 * read alone. So a processor that <i>adds</i> a refusing feature is reported faithfully, while one that <i>removes</i> the
 * feature the filter refused on leaves the twin no consumer to arm, and this reports {@code would_take: true} for a
 * request that runs two rounds. No processor in this plugin rewrites those features; core's {@code script} processor can.
 *
 * <p>The count round is the other, and it differs in the opposite direction. {@link #countSettled} has three sources, and
 * only one of them is path-dependent: totals being disabled and a leg proving the count both settle it under {@code profile}
 * exactly as without, and are read off the legs' actual answers like every other post-legs condition. The third is a
 * separate count-only round, issued for armed requests alone — armed it buys a whole round back, un-armed it would be a
 * third round costing more than the Tail it removes — which a profiled request therefore never issues. So {@code
 * count_settled} is unaffected except on the requests where only that round could have proved the count: there it stays
 * {@code false}, describing the request that ran, while the reason becomes {@link #COUNT_ROUND_NOT_RUN_UNDER_PROFILE}
 * instead of {@link #COUNT_NOT_SETTLED} when the twin <i>would</i> have issued the round — so the profile says the count is
 * derivable for the unprofiled request without claiming it was derived for this one. {@code would_take} is {@code false}
 * there and cannot be made {@code true}: whether the twin's count settles depends on what that round answers, which is
 * unknowable without issuing it. So for this one reason {@code would_take} describes the request that ran too, and the
 * reason alone speaks for the twin.
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
    /**
     * The request wants a count beyond the window, and the count-only round that would have proved it was not issued
     * because {@code profile: true} keeps two rounds and that round is only worth issuing for an armed request. Reported
     * in place of {@link #COUNT_NOT_SETTLED} only when the unprofiled twin <i>would</i> have issued it, so the two
     * reasons distinguish "no leg can prove this count" from "this count is derivable, just not for a profiled request".
     */
    public static final String COUNT_ROUND_NOT_RUN_UNDER_PROFILE = "count_round_not_run_under_profile";

    /**
     * One refusal before it is recorded: the reason and the detail that names what caused it. What the checks that read
     * a request feature by feature return, so the pair travels as one value instead of a two-element array.
     */
    public record Refusal(String reason, String detail) {
    }

    private String refusedBy;
    private String detail;
    private Long fetchEstimateBytes;
    private Long fetchBudgetBytes;
    private Boolean countSettled;
    /**
     * Whether the unprofiled twin would have issued the count-only round this request did not. Set only where it can be
     * answered — an un-armed profiled request, from the same gate the twin's count goes through — and left {@code false}
     * everywhere else, including on the armed path, where the round either ran or was refused for a reason of its own.
     * Not rendered: it selects between two {@link #refusedBy} reasons rather than being a fact about the request.
     */
    private boolean twinWouldHaveCounted;

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

    /** {@link #refuse(String, String)} for a refusal already established as a pair. */
    public FastPathDecision refuse(final Refusal refusal) {
        return refuse(refusal.reason(), refusal.detail());
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
