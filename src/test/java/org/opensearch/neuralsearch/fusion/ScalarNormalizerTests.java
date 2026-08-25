/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil;
import org.opensearch.neuralsearch.processor.normalization.L2ScoreNormalizer;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizer;
import org.opensearch.neuralsearch.processor.normalization.RRFScoreNormalizer;
import org.opensearch.neuralsearch.processor.normalization.ZScoreNormalizer;
import org.opensearch.test.OpenSearchTestCase;

public class ScalarNormalizerTests extends OpenSearchTestCase {

    private static final float DELTA = 1e-6f;

    private ScoreCombinationTechnique arithmeticMean() {
        return new ArithmeticMeanScoreCombinationTechnique(Map.of(), new ScoreCombinationUtil());
    }

    private Map<String, Float> leg(Object... idScorePairs) {
        Map<String, Float> leg = new LinkedHashMap<>();
        for (int i = 0; i < idScorePairs.length; i += 2) {
            leg.put((String) idScorePairs[i], (Float) idScorePairs[i + 1]);
        }
        return leg;
    }

    // ---- technique lookup ----

    public void testForTechnique_resolvesMinMax() {
        ScalarNormalizer normalizer = ScalarNormalizers.forTechnique("min_max");
        assertSame(MinMaxScalarNormalizer.INSTANCE, normalizer);
        assertEquals("min_max", normalizer.techniqueName());
    }

    public void testForTechnique_resolvesZScore() {
        ScalarNormalizer normalizer = ScalarNormalizers.forTechnique("z_score");
        assertSame(ZScoreScalarNormalizer.INSTANCE, normalizer);
        assertEquals("z_score", normalizer.techniqueName());
    }

    public void testForTechnique_resolvesL2() {
        ScalarNormalizer normalizer = ScalarNormalizers.forTechnique("l2");
        assertSame(L2ScalarNormalizer.INSTANCE, normalizer);
        assertEquals("l2", normalizer.techniqueName());
    }

    public void testForTechnique_resolvesRrf() {
        // rrf resolves through the same lookup as the score-based techniques rather than through a branch of its own; that
        // is what makes it the extension point the other rank-based techniques would use.
        ScalarNormalizer normalizer = ScalarNormalizers.forTechnique(
            "rrf",
            Map.of(RRFScoreNormalizer.PARAM_NAME_RANK_CONSTANT, RRFScoreNormalizer.DEFAULT_RANK_CONSTANT)
        );
        assertTrue(normalizer instanceof RrfScalarNormalizer);
        assertEquals("rrf", normalizer.techniqueName());
    }

    public void testForTechnique_whenRrfWithoutParameters_thenDefaultRankConstant() {
        // The no-parameter overload has to stay usable for rrf too, because the score-ranker-processor shape supplies no
        // rank_constant; it must fall back to the same default classic uses rather than reject the lookup.
        Map<String, Float> withDefault = ScalarNormalizers.forTechnique("rrf").normalizeLeg(leg("a", 2.0f, "b", 1.0f));
        assertEquals(RRFScoreNormalizer.scoreForRank(0, RRFScoreNormalizer.DEFAULT_RANK_CONSTANT), withDefault.get("a"), 0.0f);
    }

    public void testForTechnique_whenRrfWithRankConstant_thenParameterIsHonored() {
        // Names map to factories precisely so a parameterized technique can be built per-query: two different rank
        // constants must produce two differently-scored normalizers, not one shared singleton.
        Map<String, Float> raw = leg("a", 2.0f, "b", 1.0f);

        Map<String, Float> small = ScalarNormalizers.forTechnique("rrf", Map.of(RRFScoreNormalizer.PARAM_NAME_RANK_CONSTANT, 1))
            .normalizeLeg(raw);
        Map<String, Float> large = ScalarNormalizers.forTechnique("rrf", Map.of(RRFScoreNormalizer.PARAM_NAME_RANK_CONSTANT, 100))
            .normalizeLeg(raw);

        assertEquals(RRFScoreNormalizer.scoreForRank(0, 1), small.get("a"), 0.0f);
        assertEquals(RRFScoreNormalizer.scoreForRank(0, 100), large.get("a"), 0.0f);
        assertTrue(small.get("a") > large.get("a"));
    }

    public void testSupportedTechniques_isTheScoreNormalizationFamilyPlusRrf() {
        // The caller's rewrite-time gate refuses anything outside this set, so it doubles as the fused-mode scope
        // statement. `none` is deliberately absent: a fusion config that names no normalization has nothing to resolve.
        assertEquals(Set.of("min_max", "z_score", "l2", "rrf"), ScalarNormalizers.supportedTechniques());
    }

    public void testForTechnique_whenTechniqueNotWiredYet_thenThrows() {
        // The caller rejects these at rewrite, making this the defense-in-depth backstop.
        for (String notWired : List.of("none", "not_a_technique")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ScalarNormalizers.forTechnique(notWired));
            assertTrue(e.getMessage().contains("is not supported in fused mode"));
        }
    }

    public void testForTechnique_whenNullTechnique_thenThrows() {
        // A null technique reaches the same refusal rather than an NPE from the immutable lookup map.
        expectThrows(IllegalArgumentException.class, () -> ScalarNormalizers.forTechnique(null));
    }

    // ---- min_max normalizer ----

    public void testMinMaxNormalizeLeg_matchesSharedScalarMath() {
        // The normalizer must be a pure re-expression of the shared classic math, per score.
        Map<String, Float> raw = leg("a", 1.0f, "b", 1.5f, "c", 2.0f);

        Map<String, Float> normalized = MinMaxScalarNormalizer.INSTANCE.normalizeLeg(raw);

        assertEquals(raw.keySet(), normalized.keySet());
        assertEquals(MinMaxScoreNormalizer.normalizeSingleScore(1.0f, 1.0f, 2.0f), normalized.get("a"), DELTA);
        assertEquals(MinMaxScoreNormalizer.normalizeSingleScore(1.5f, 1.0f, 2.0f), normalized.get("b"), DELTA);
        assertEquals(MinMaxScoreNormalizer.normalizeSingleScore(2.0f, 1.0f, 2.0f), normalized.get("c"), DELTA);
    }

    public void testMinMaxNormalizeLeg_whenEmptyLeg_thenEmptyResult() {
        // A leg that matched nothing must stay empty — CoordinatorScoreFusion reads a missing key as "leg didn't match".
        assertTrue(MinMaxScalarNormalizer.INSTANCE.normalizeLeg(Map.of()).isEmpty());
    }

    public void testMinMaxNormalizeLeg_whenSingleScore_thenOne() {
        // min == max single-result edge case: classic returns 1.0.
        Map<String, Float> normalized = MinMaxScalarNormalizer.INSTANCE.normalizeLeg(leg("only", 0.7f));
        assertEquals(1.0f, normalized.get("only"), DELTA);
    }

    public void testMinMaxNormalizeLeg_preservesEveryKey() {
        Map<String, Float> raw = leg("a", 0.1f, "b", 0.2f, "c", 0.3f, "d", 0.4f);
        assertEquals(raw.size(), MinMaxScalarNormalizer.INSTANCE.normalizeLeg(raw).size());
    }

    // ---- z_score normalizer ----

    public void testZScoreNormalizeLeg_matchesSharedScalarMath() {
        // Same contract as min_max: a pure re-expression of the shared classic math, per score. The statistics must be
        // the ones the shared accumulator computes over the leg, so the reference builds them the same way.
        Map<String, Float> raw = leg("a", 1.0f, "b", 4.0f, "c", 10.0f);
        ZScoreNormalizer.StatsAccumulator statistics = new ZScoreNormalizer.StatsAccumulator();
        raw.values().forEach(statistics::add);

        Map<String, Float> normalized = ZScoreScalarNormalizer.INSTANCE.normalizeLeg(raw);

        assertEquals(raw.keySet(), normalized.keySet());
        for (Map.Entry<String, Float> entry : raw.entrySet()) {
            float expected = ZScoreNormalizer.normalizeSingleScore(
                entry.getValue(),
                statistics.standardDeviation(),
                statistics.mean(),
                statistics.max(),
                statistics.min()
            );
            assertEquals(Float.floatToIntBits(expected), Float.floatToIntBits(normalized.get(entry.getKey())));
        }
    }

    public void testZScoreNormalizeLeg_whenEmptyLeg_thenEmptyResult() {
        // A leg that matched nothing must stay empty; in particular no statistic is read off an empty accumulator.
        assertTrue(ZScoreScalarNormalizer.INSTANCE.normalizeLeg(Map.of()).isEmpty());
    }

    public void testZScoreNormalizeLeg_whenSingleScore_thenSubqueryMax() {
        // mean == score single-result edge case: classic returns the sub-query max, which for one hit is the score.
        Map<String, Float> normalized = ZScoreScalarNormalizer.INSTANCE.normalizeLeg(leg("only", 0.7f));
        assertEquals(0.7f, normalized.get("only"), DELTA);
    }

    public void testZScoreNormalizeLeg_preservesEveryKey() {
        Map<String, Float> raw = leg("a", 0.1f, "b", 0.2f, "c", 0.3f, "d", 0.4f);
        assertEquals(raw.size(), ZScoreScalarNormalizer.INSTANCE.normalizeLeg(raw).size());
    }

    // ---- l2 normalizer ----

    public void testL2NormalizeLeg_matchesSharedScalarMath() {
        // 3^2 + 4^2 = 25, so the leg norm is 5 and each score is divided by it.
        Map<String, Float> raw = leg("a", 3.0f, "b", 4.0f);

        Map<String, Float> normalized = L2ScalarNormalizer.INSTANCE.normalizeLeg(raw);

        assertEquals(raw.keySet(), normalized.keySet());
        assertEquals(Float.floatToIntBits(0.6f), Float.floatToIntBits(normalized.get("a")));
        assertEquals(Float.floatToIntBits(0.8f), Float.floatToIntBits(normalized.get("b")));
    }

    public void testL2NormalizeLeg_whenEmptyLeg_thenEmptyResult() {
        assertTrue(L2ScalarNormalizer.INSTANCE.normalizeLeg(Map.of()).isEmpty());
    }

    public void testL2NormalizeLeg_whenAllScoresZero_thenMinScore() {
        // A whole leg of zero scores has a zero norm; the shared core guards the division rather than emitting NaN.
        Map<String, Float> normalized = L2ScalarNormalizer.INSTANCE.normalizeLeg(leg("a", 0.0f, "b", 0.0f));
        assertEquals(L2ScoreNormalizer.MIN_SCORE, normalized.get("a"), DELTA);
        assertEquals(L2ScoreNormalizer.MIN_SCORE, normalized.get("b"), DELTA);
    }

    public void testL2NormalizeLeg_preservesEveryKey() {
        Map<String, Float> raw = leg("a", 0.1f, "b", 0.2f, "c", 0.3f, "d", 0.4f);
        assertEquals(raw.size(), L2ScalarNormalizer.INSTANCE.normalizeLeg(raw).size());
    }

    // ---- rrf normalizer ----

    public void testRrfNormalizeLeg_replacesScoresWithRankScores() {
        // Unlike the score-based normalizers, the output is a function of order alone: the shared rank arithmetic applied to
        // each score's position within the leg, descending.
        Map<String, Float> raw = leg("a", 0.2f, "b", 9.0f, "c", 1.0f);

        Map<String, Float> normalized = new RrfScalarNormalizer(RRFScoreNormalizer.DEFAULT_RANK_CONSTANT).normalizeLeg(raw);

        assertEquals(raw.keySet(), normalized.keySet());
        int k = RRFScoreNormalizer.DEFAULT_RANK_CONSTANT;
        assertEquals(RRFScoreNormalizer.scoreForRank(2, k), normalized.get("a"), 0.0f);
        assertEquals(RRFScoreNormalizer.scoreForRank(0, k), normalized.get("b"), 0.0f);
        assertEquals(RRFScoreNormalizer.scoreForRank(1, k), normalized.get("c"), 0.0f);
    }

    public void testRrfNormalizeLeg_whenEmptyLeg_thenEmptyResult() {
        assertTrue(new RrfScalarNormalizer(RRFScoreNormalizer.DEFAULT_RANK_CONSTANT).normalizeLeg(Map.of()).isEmpty());
    }

    public void testRrfNormalizeLeg_ignoresScoreMagnitude() {
        // Two legs with the same ordering but wildly different score scales must normalize identically — the property that
        // makes rrf immune to the incomparable-score problem the score-based techniques exist to solve.
        RrfScalarNormalizer normalizer = new RrfScalarNormalizer(RRFScoreNormalizer.DEFAULT_RANK_CONSTANT);

        assertEquals(normalizer.normalizeLeg(leg("a", 2.0f, "b", 1.0f)), normalizer.normalizeLeg(leg("a", 9000.0f, "b", 0.001f)));
    }

    public void testRrfNormalizeLeg_preservesEveryKey() {
        Map<String, Float> raw = leg("a", 0.1f, "b", 0.2f, "c", 0.3f, "d", 0.4f);
        assertEquals(raw.size(), new RrfScalarNormalizer(RRFScoreNormalizer.DEFAULT_RANK_CONSTANT).normalizeLeg(raw).size());
    }

    // ---- fuse(normalizer) is behavior-identical to the fuseMinMax it replaced ----

    public void testFuse_withMinMaxNormalizer_equalsFuseMinMax() {
        // Locks in that routing normalization through the ScalarNormalizer seam changed nothing: same fused scores,
        // same key order. This is the guard that the refactor stayed behavior-preserving.
        List<Map<String, Float>> legs = List.of(leg("1", 0.9f, "2", 0.5f), leg("2", 0.8f, "3", 0.4f));

        Map<String, Float> viaSeam = CoordinatorScoreFusion.fuse(legs, MinMaxScalarNormalizer.INSTANCE, arithmeticMean());
        Map<String, Float> viaConvenience = CoordinatorScoreFusion.fuseMinMax(legs, arithmeticMean());

        assertEquals(viaConvenience.keySet(), viaSeam.keySet());
        assertEquals(List.copyOf(viaConvenience.keySet()), List.copyOf(viaSeam.keySet()));
        for (String id : viaConvenience.keySet()) {
            assertEquals(viaConvenience.get(id), viaSeam.get(id), 0.0f);
        }
    }

    public void testFuse_withCustomNormalizer_isPluggedIn() {
        // A stand-in normalizer proves the seam is real: fusion uses whatever the normalizer returns, and a key the
        // normalizer omits is treated as "leg did not match" (0.0 slot).
        ScalarNormalizer constantHalf = new ScalarNormalizer() {
            @Override
            public Map<String, Float> normalizeLeg(Map<String, Float> legRawScores) {
                Map<String, Float> out = new LinkedHashMap<>();
                legRawScores.keySet().forEach(id -> out.put(id, 0.5f));
                return out;
            }

            @Override
            public String techniqueName() {
                return "constant_half";
            }
        };
        List<Map<String, Float>> legs = List.of(leg("1", 0.9f), leg("1", 0.2f));

        Map<String, Float> fused = CoordinatorScoreFusion.fuse(legs, constantHalf, arithmeticMean());

        // Both legs matched doc 1 and both normalized to 0.5 → unweighted arithmetic mean is 0.5.
        assertEquals(0.5f, fused.get("1"), DELTA);
    }

    public void testFuse_whenKeysAreOpaqueComposites_thenCarriedThroughUnchanged() {
        // Keys are opaque to fusion and to normalizers: a composite identity string round-trips untouched, which is what
        // lets the caller change its document identity scheme without touching this layer.
        List<Map<String, Float>> legs = List.of(leg("idx-a#1", 0.9f), leg("idx-b#1", 0.8f));

        Map<String, Float> fused = CoordinatorScoreFusion.fuse(legs, MinMaxScalarNormalizer.INSTANCE, arithmeticMean());

        assertEquals(2, fused.size());
        assertTrue(fused.containsKey("idx-a#1"));
        assertTrue(fused.containsKey("idx-b#1"));
    }
}
