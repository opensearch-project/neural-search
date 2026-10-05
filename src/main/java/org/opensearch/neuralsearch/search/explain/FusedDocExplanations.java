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
     * enclosing {@code boost} scales it. So this says what round 2 returned rather than naming a cause.
     */
    private static final String FINAL_SCORE_DESCRIPTION = "score of the fused hybrid query as round 2 returned it, computed from:";

    /**
     * The three descriptions core's {@code QueryRescorer#explain} writes, and the only ones this class pattern-matches on.
     * A rescore layer is {@code match(v, "<score_mode> of:", prim, sec)} when the rescore query matched the document and
     * just {@code prim} when it did not, where {@code prim = match(source * query_weight, "product of:", source,
     * match(query_weight, "primaryWeight"))} and {@code sec = match(r * rescore_query_weight, "product of:", r,
     * match(rescore_query_weight, "secondaryWeight"))}. {@code source} is what the layer rescored: the first-pass query's
     * own explanation for the first rescorer, the previous layer's whole tree for every rescorer after it.
     */
    private static final String RESCORE_PRODUCT_DESCRIPTION = "product of:";
    private static final String RESCORE_PRIMARY_WEIGHT_DESCRIPTION = "primaryWeight";
    private static final String RESCORE_SECONDARY_WEIGHT_DESCRIPTION = "secondaryWeight";

    /**
     * What round 2 will be told, per document key, in leg order. Empty for an unexplained request, and absent for a
     * document that fusion did not rank (one the Tail surfaced) — {@link FusedExplanationMerger} leaves those alone.
     */
    private final Map<String, List<LegContribution>> contributionsByKey = new LinkedHashMap<>();

    /** The fused score fusion computed per document key, before a rescore moved it. */
    private final Map<String, Float> fusedScoreByKey = new LinkedHashMap<>();

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
     */
    public void addDocument(final String documentKey, final float fusedScore, final List<LegContribution> contributions) {
        fusedScoreByKey.put(documentKey, fusedScore);
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
     * <p>When the hit's score is the fused score, round 2 added nothing and the fused combination is the whole tree.
     * When they differ because a {@code rescore} moved the score, round 2's tree already describes the move exactly the
     * way core describes it for any other query — {@code "<score_mode> of:"} over the weighted first pass and the
     * weighted rescore query — except that its first pass is the self-erased {@code _id} clause carrying the fused
     * score as a bare boost. So that tree is kept and the fused combination replaces that first pass, which is the one
     * node in it that describes a number the user's query did not compute. The result reads as core's own
     * rescore explanation would over a classic query: {@code sum of: [product of: [<fused combination>, primaryWeight],
     * product of: [<rescore query>, secondaryWeight]]}. Chained rescorers nest the same way, each layer's first pass
     * being the previous layer's tree, so the replacement descends through them to the one first pass that is the
     * query's.
     *
     * <p>The replacement is conservative. It is attempted only when the score moved, it requires the first pass it
     * replaces to carry exactly the fused score, and it requires the rebuilt tree to still describe the hit's score; when round 2's
     * tree is not a rescore tree (something else moved the score), or does not satisfy either check, the fusion is
     * nested under a node naming the final score, as before — correct, just silent about the cause.
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
        if (Float.isNaN(hitScore) || Float.compare(fusedScore, hitScore) == 0) {
            return combination;
        }
        if (Objects.nonNull(roundTwo)) {
            Explanation rescored = replaceFirstPass(roundTwo, combination, fusedScore);
            if (Objects.nonNull(rescored) && Float.compare(rescored.getValue().floatValue(), hitScore) == 0) {
                return rescored;
            }
        }
        return Explanation.match(hitScore, FINAL_SCORE_DESCRIPTION, List.of(combination));
    }

    /**
     * Round 2's rescore tree with the fused combination in place of the first pass it rescored, or {@code null} when
     * {@code node} is not a rescore layer or its innermost first pass does not carry the fused score. Pure: the input
     * tree is never mutated, every node on the path to the replaced first pass is rebuilt with its own value and description.
     */
    static Explanation replaceFirstPass(final Explanation node, final Explanation combination, final float fusedScore) {
        if (isWeightedFirstPass(node)) {
            Explanation[] details = node.getDetails();
            Explanation source = details[0];
            Explanation replaced;
            if (isRescoreLayer(source)) {
                // A chained rescorer: what this layer rescored is the previous layer's whole tree.
                replaced = replaceFirstPass(source, combination, fusedScore);
            } else {
                // The first rescorer: what it rescored is the self-erased query, whose score is the fused score exactly —
                // the Top clause carries it as its boost and the Tail contributes 0.0 to the sum.
                replaced = Float.compare(source.getValue().floatValue(), fusedScore) == 0 ? combination : null;
            }
            if (Objects.isNull(replaced)) {
                return null;
            }
            return Explanation.match(node.getValue().floatValue(), node.getDescription(), replaced, details[1]);
        }
        if (isCombinedRescore(node)) {
            Explanation[] details = node.getDetails();
            Explanation primary = replaceFirstPass(details[0], combination, fusedScore);
            if (Objects.isNull(primary)) {
                return null;
            }
            return Explanation.match(node.getValue().floatValue(), node.getDescription(), primary, details[1]);
        }
        return null;
    }

    private static boolean isRescoreLayer(final Explanation node) {
        return isWeightedFirstPass(node) || isCombinedRescore(node);
    }

    /** {@code match(source * query_weight, "product of:", source, match(query_weight, "primaryWeight"))}. */
    private static boolean isWeightedFirstPass(final Explanation node) {
        return isWeightedProduct(node, RESCORE_PRIMARY_WEIGHT_DESCRIPTION);
    }

    /** {@code match(r * rescore_query_weight, "product of:", r, match(rescore_query_weight, "secondaryWeight"))}. */
    private static boolean isWeightedRescoreQuery(final Explanation node) {
        return isWeightedProduct(node, RESCORE_SECONDARY_WEIGHT_DESCRIPTION);
    }

    private static boolean isWeightedProduct(final Explanation node, final String weightDescription) {
        if (node.isMatch() == false || RESCORE_PRODUCT_DESCRIPTION.equals(node.getDescription()) == false) {
            return false;
        }
        Explanation[] details = node.getDetails();
        return details.length == 2 && weightDescription.equals(details[1].getDescription());
    }

    /**
     * {@code match(v, "<score_mode> of:", prim, sec)}: recognised by its two children, not its description, because
     * {@code "sum of:"} is also what a plain {@code bool} query's explanation says.
     */
    private static boolean isCombinedRescore(final Explanation node) {
        if (node.isMatch() == false) {
            return false;
        }
        Explanation[] details = node.getDetails();
        return details.length == 2 && isWeightedFirstPass(details[0]) && isWeightedRescoreQuery(details[1]);
    }
}
