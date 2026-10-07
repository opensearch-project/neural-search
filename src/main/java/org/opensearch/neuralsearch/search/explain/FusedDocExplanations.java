/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.explain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.Explanation;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

/**
 * How each document in a fused ({@code fusion}) hybrid's window earned its fused score: per leg, that leg's own raw
 * Lucene explanation from round 1 and the normalized value fusion derived from it.
 *
 * <p>Fused mode normalizes and combines on the <b>coordinator</b>, so the query round 2 runs carries the fused score as a
 * childless {@code constant_score} clause — the right number with nothing under it. Everything needed to describe how
 * that number was reached exists only during the rewrite, and is discarded there: the legs' explanations arrive on the
 * leg hits and the per-leg normalized values are local to {@code CoordinatorScoreFusion}. This class is where both are
 * kept so {@link FusedExplanationMerger} can rebuild the tree on the response.
 *
 * <p>Mutable and single-writer: one instance per fused hybrid per request, written on the leg MultiSearch response
 * thread while the fused query is built, then read once when the response comes back. Always constructed, even when the
 * request did not ask to be explained, so the orchestrator never has to null-check; an unexplained request records
 * nothing (its legs ran without {@code explain}, so there are no explanations to record) and the instance is thrown
 * away.
 */
@Log4j2
public final class FusedDocExplanations {

    /**
     * Separator for the composite {@code _index} + {@code _id} document key. Lives here rather than in the orchestrator
     * because the key has to be built in two places — over a leg hit during the rewrite, and over a response hit when
     * the explanation is attached — and one definition is what keeps them the same key.
     */
    private static final String KEY_SEPARATOR = "#";

    /**
     * Description for the extra node inserted when the score round 2 returned is not the fused score and round 2's own
     * explanation does not say why (see {@link #explain(String, float, Explanation)}). Naming the combination node with
     * the final score would claim the fusion produced a number it did not.
     *
     * <p>Worded for every reason the two can differ: the score is also moved when
     * {@code HybridFusionOrchestrator#scoreAboveTail} floors a degenerate fused score away from {@code 0.0}, and when an
     * enclosing {@code boost} scales it. So this says what round 2 returned rather than naming a cause. The same node
     * stands in for a floored first pass inside a rescore explanation, where it carries the floored value round 2 ran with.
     */
    private static final String FINAL_SCORE_DESCRIPTION = "score of the fused hybrid query as round 2 returned it, computed from:";

    /**
     * What round 2 will be told, per document key, in leg order. Empty for an unexplained request, and absent for a
     * document that fusion did not rank (one the Tail surfaced) — {@link FusedExplanationMerger} leaves those alone.
     */
    private final Map<String, List<LegContribution>> contributionsByKey = new LinkedHashMap<>();

    /** The fused score fusion computed per document key, before a rescore moved it. */
    private final Map<String, Float> fusedScoreByKey = new LinkedHashMap<>();

    /**
     * The score round 2 runs with per document key: the fused score after {@code HybridFusionOrchestrator#scoreAboveTail},
     * which is the boost of the document's Top clause and therefore the value of its first pass.
     */
    private final Map<String, Float> roundTwoScoreByKey = new LinkedHashMap<>();

    /**
     * How far, in ulps, a rebuilt rescore explanation may sit from the hit's score and still describe it. Core explains
     * some rescore queries in float arithmetic that they score in double — {@code function_score} and {@code script_score}
     * combine their factors in double when scoring and in float when explaining — so a correct tree can land one or two
     * ulps away. Everything the check exists to reject (a layer the score never went through, a band value, a score some
     * later step moved) is off by far more.
     */
    private static final int ROOT_TOLERANCE_ULPS = 4;

    /**
     * Description for the combination node, in classic hybrid's exact wording — see
     * {@code ScoreCombiner#explainByShard}, which formats {@code "%s combination of:"} over the technique's own
     * {@code describe()}.
     */
    @Getter
    @Setter
    @Accessors(chain = true, fluent = true)
    private String combinationDescription;

    /**
     * Description for each per-leg normalization node, in classic hybrid's exact wording — see
     * {@code ExplanationUtils#getDocIdAtQueryForNormalization}, which formats {@code "%s normalization of:"} over the
     * technique's own {@code describe()}, exactly as the combination node above does. {@code describe()} and not the
     * technique's name: they differ for {@code rrf}, whose description carries the rank constant.
     */
    @Getter
    @Setter
    @Accessors(chain = true, fluent = true)
    private String normalizationDescription;

    /**
     * One leg's share of a document's fused score: the value fusion actually combined, and the leg's own explanation of
     * the raw score it was derived from.
     *
     * @param legIndex        the leg's position in the hybrid, as written
     * @param normalizedScore what normalization turned this leg's raw score into — the value the combiner consumed
     * @param rawExplanation  the leg's own Lucene explanation from round 1, or {@code null} when the leg ran without
     *                        {@code explain} or the shard returned none
     */
    public record LegContribution(int legIndex, float normalizedScore, Explanation rawExplanation) {
    }

    /** The fusion key for a document: its {@code _index}, the separator, and its {@code _id}. Never parsed back. */
    public static String documentKey(final String index, final String id) {
        return index + KEY_SEPARATOR + id;
    }

    /**
     * Record one document's breakdown. Called once per ranked document, with one contribution per leg that matched it —
     * a leg that did not match contributes nothing rather than a zero node, matching classic hybrid, which likewise only
     * renders the legs whose own explanation is a match.
     *
     * @param fusedScore    the score fusion computed, which the combination node is labelled with
     * @param roundTwoScore the score round 2 runs with, which differs from {@code fusedScore} only where it was floored
     */
    public void addDocument(
        final String documentKey,
        final float fusedScore,
        final float roundTwoScore,
        final List<LegContribution> contributions
    ) {
        fusedScoreByKey.put(documentKey, fusedScore);
        roundTwoScoreByKey.put(documentKey, roundTwoScore);
        contributionsByKey.put(documentKey, List.copyOf(contributions));
    }

    /** True when nothing was recorded, i.e. the request did not ask to be explained (or fusion ranked nothing). */
    public boolean isEmpty() {
        return contributionsByKey.isEmpty();
    }

    /**
     * The tree for one document, or {@code null} when this document was not ranked by fusion. {@code hitScore} is the
     * score round 2 actually returned: it differs from the fused score when a {@code rescore} moved it, and in that case
     * the fused combination becomes a child of a node describing the final score rather than being relabelled as one.
     *
     * <p>A {@code NaN} {@code hitScore} is a request that sorts by a field without tracking scores. There is no final
     * score to describe there, so the fusion is reported on its own — wrapping it in a node claiming a score of zero
     * would describe a number nothing computed.
     *
     * <p>Equivalent to {@link #explain(String, float, Explanation)} with no round-2 explanation: the fusion is nested
     * under the final score with nothing said about what moved it.
     */
    public Explanation explain(final String documentKey, final float hitScore) {
        return explain(documentKey, hitScore, null);
    }

    /**
     * The tree for one document given round 2's own explanation of it, or {@code null} when this document was not
     * ranked by fusion.
     *
     * <p>When round 2's explanation carries a {@link FusedFirstPassMarker}, a rescore ran: the fused rescore guard marked
     * the first pass before core's rescorers wrapped it, and the tree around it is core's own explanation of that rescore,
     * whatever its layers say. The marker is replaced by the fused breakdown — the combination, or the combination under
     * the final-score node at the floored value when round 2 ran with a floored score — and the rest of the tree is kept,
     * so a rescored hit reads the way core explains any rescored query: {@code sum of: [product of: [<fused combination>,
     * primaryWeight], product of: [<rescore query>, secondaryWeight]]}, chained rescorers nesting the same way. The result
     * is kept only when the marker carries exactly the score round 2 ran with and the rebuilt tree still describes the
     * hit's score, to within {@link #ROOT_TOLERANCE_ULPS} ulps; anything else — something that added score to round 2's
     * query, say — is answered as if there were no marker, and logged at debug with the numbers that disagreed.
     *
     * <p>The tree shows the layers core built from which documents each rescorer rescored. For a chain of rescorers
     * that reached different documents on an index with more than one shard, the shards report those sets as one union
     * (see {@code FusedWindowGuardRescorerBuilder}), so a layer the score never went through can appear; when it moves the
     * value the check above rejects the tree, and when it does not — a {@code max} that keeps the first pass — it is shown.
     *
     * <p>Without a marker no rescore ran (or a shard that does not mark answered): when the hit's score is the fused score,
     * round 2 added nothing and the fused combination is the whole tree; otherwise the fusion is nested under the
     * final-score node, which is correct, just silent about the cause.
     */
    public Explanation explain(final String documentKey, final float hitScore, final Explanation roundTwo) {
        List<LegContribution> contributions = contributionsByKey.get(documentKey);
        if (Objects.isNull(contributions)) {
            return null;
        }
        List<Explanation> legDetails = new ArrayList<>(contributions.size());
        for (LegContribution contribution : contributions) {
            Explanation raw = contribution.rawExplanation();
            legDetails.add(
                Explanation.match(contribution.normalizedScore(), normalizationDescription, Objects.isNull(raw) ? List.of() : List.of(raw))
            );
        }
        float fusedScore = fusedScoreByKey.get(documentKey);
        Explanation combination = Explanation.match(fusedScore, combinationDescription, legDetails);
        if (Float.isNaN(hitScore)) {
            return combination;
        }
        if (Objects.nonNull(roundTwo)) {
            float roundTwoScore = roundTwoScoreByKey.get(documentKey);
            Explanation firstPass = Float.compare(roundTwoScore, fusedScore) == 0
                ? combination
                : Explanation.match(roundTwoScore, FINAL_SCORE_DESCRIPTION, List.of(combination));
            Explanation rescored = FusedFirstPassMarker.replace(
                roundTwo,
                marker -> Float.compare(marker.getValue().floatValue(), roundTwoScore) == 0 ? firstPass : null
            );
            if (rescored != roundTwo) {
                if (Objects.nonNull(rescored) && describes(rescored, hitScore)) {
                    return rescored;
                }
                logRescoreTreeNotKept(documentKey, hitScore, roundTwoScore, rescored);
            }
        }
        if (Float.compare(fusedScore, hitScore) == 0) {
            return combination;
        }
        return Explanation.match(hitScore, FINAL_SCORE_DESCRIPTION, List.of(combination));
    }

    /** Whether a rebuilt tree describes the hit's score: exactly, or — for finite values — within a few ulps. */
    private static boolean describes(final Explanation tree, final float hitScore) {
        float value = tree.getValue().floatValue();
        if (Float.isFinite(value) == false || Float.isFinite(hitScore) == false) {
            return Float.compare(value, hitScore) == 0;
        }
        return Math.abs(value - hitScore) <= ROOT_TOLERANCE_ULPS * Math.ulp(Math.max(Math.abs(value), Math.abs(hitScore)));
    }

    /**
     * Round 2's rescore explanation was not kept for this document, so the hit reads as it would with no rescore
     * explanation at all: still the right number on top, just silent about what moved it. Debug and not a warning: it
     * fires per hit of an {@code explain} request, the response stays correct, and the one known cause — a chain of
     * rescorers that rescored different documents on an index with more than one shard, see
     * {@code FusedWindowGuardRescorerBuilder} — is the request's own shape. The numbers are what a report needs.
     *
     * @param rescored the rebuilt tree, or {@code null} when the marker did not carry the score round 2 ran with
     */
    private static void logRescoreTreeNotKept(
        final String documentKey,
        final float hitScore,
        final float roundTwoScore,
        final Explanation rescored
    ) {
        if (Objects.isNull(rescored)) {
            log.debug(
                "fused hybrid explain: the marked first pass of [{}] does not carry the score round 2 ran with ({}); "
                    + "naming the hit's score {} over the fused breakdown instead of keeping the rescore explanation",
                documentKey,
                roundTwoScore,
                hitScore
            );
            return;
        }
        float value = rescored.getValue().floatValue();
        log.debug(
            "fused hybrid explain: the rescore explanation of [{}] rebuilds to {} but the hit scored {} ({} ulps apart, "
                + "tolerance {}); naming the hit's score over the fused breakdown instead",
            documentKey,
            value,
            hitScore,
            Math.abs(value - hitScore) / Math.ulp(Math.max(Math.abs(value), Math.abs(hitScore))),
            ROOT_TOLERANCE_ULPS
        );
    }
}
