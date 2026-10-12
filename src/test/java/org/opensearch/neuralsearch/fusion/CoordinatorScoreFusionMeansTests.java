/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.List;
import java.util.Map;

import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@code geometric_mean} and {@code harmonic_mean} through the coordinator fusion path, against scores computed by hand.
 *
 * <p>The fixture is deliberately arithmetic rather than lexical: two legs whose raw scores are exact, chosen so that
 * {@code min_max} maps them to {@code 1.0} and to the {@code 0.001} floor. That makes every expected value below
 * derivable on paper, which is the only way a combination test can catch a plausible-but-wrong formula — a test that
 * asserts "whatever the code produced" would pass for any of the three means.
 *
 * <p>Leg scores: leg A {@code {d1: 1.0, d2: 0.5}}, leg B {@code {d2: 1.0, d3: 0.25}}. After per-leg min_max:
 * <ul>
 *   <li>leg A: d1 = (1.0-0.5)/(1.0-0.5) = 1.0, d2 = 0 -> floored to 0.001</li>
 *   <li>leg B: d2 = (1.0-0.25)/(1.0-0.25) = 1.0, d3 = 0 -> floored to 0.001</li>
 * </ul>
 * A leg a document did not match contributes {@code 0.0}, which all three means skip, so d1 and d3 are single-leg.
 */
public class CoordinatorScoreFusionMeansTests extends OpenSearchTestCase {

    private static final List<Map<String, Float>> LEGS = List.of(Map.of("d1", 1.0f, "d2", 0.5f), Map.of("d2", 1.0f, "d3", 0.25f));
    /** Default weights are equal, so each contributing leg carries 0.5. */
    private static final double W = 0.5;
    private static final float FLOOR = 0.001f;

    private static Map<String, Float> fuse(String technique) {
        ScoreCombinationTechnique combination = new ScoreCombinationFactory().createCombination(technique);
        return CoordinatorScoreFusion.fuseMinMax(LEGS, combination);
    }

    public void testGeometricMean_matchesScoresComputedByHand() {
        // d2 is the only two-leg document: exp((0.5*ln(0.001) + 0.5*ln(1.0)) / (0.5+0.5)) = sqrt(0.001)
        double expectedD2 = Math.exp((W * Math.log(FLOOR) + W * Math.log(1.0)) / (W + W));
        assertEquals("sqrt of the floor, since the other leg contributes ln(1)=0", Math.sqrt(FLOOR), expectedD2, 1e-9);

        Map<String, Float> fused = fuse("geometric_mean");

        assertEquals(3, fused.size());
        // Single-leg documents reduce to their own normalized score: exp(w*ln(x)/w) = x.
        assertEquals(1.0f, fused.get("d1"), 1e-6f);
        assertEquals((float) expectedD2, fused.get("d2"), 1e-6f);
        assertEquals(FLOOR, fused.get("d3"), 1e-6f);
    }

    public void testHarmonicMean_matchesScoresComputedByHand() {
        // d2: (0.5+0.5) / (0.5/0.001 + 0.5/1.0) = 1 / 500.5
        double expectedD2 = (W + W) / (W / FLOOR + W / 1.0);
        assertEquals(1.0 / 500.5, expectedD2, 1e-9);

        Map<String, Float> fused = fuse("harmonic_mean");

        assertEquals(3, fused.size());
        assertEquals(1.0f, fused.get("d1"), 1e-6f);
        assertEquals((float) expectedD2, fused.get("d2"), 1e-9f);
        assertEquals(FLOOR, fused.get("d3"), 1e-6f);
    }

    /**
     * The three means must order this fixture differently, or the test above proves nothing about which formula ran:
     * arithmetic puts the two-leg document first, while geometric and harmonic both punish its floored leg enough to drop
     * it behind the single-leg document that scored 1.0.
     */
    public void testTheThreeMeansAreDistinguishable() {
        Map<String, Float> arithmetic = fuse("arithmetic_mean");
        Map<String, Float> geometric = fuse("geometric_mean");
        Map<String, Float> harmonic = fuse("harmonic_mean");

        assertTrue("arithmetic ranks the two-leg document above the single-leg one", arithmetic.get("d2") > arithmetic.get("d1"));
        assertTrue("geometric does not", geometric.get("d2") < geometric.get("d1"));
        assertTrue("harmonic does not either", harmonic.get("d2") < harmonic.get("d1"));
        assertTrue("and harmonic punishes the floored leg harder than geometric", harmonic.get("d2") < geometric.get("d2"));
    }

    /** Every mean must keep the fused score finite, which is what the round-2 Top clause and the page assembly assume. */
    public void testNoMeanProducesANonFiniteScore() {
        for (String technique : List.of("arithmetic_mean", "geometric_mean", "harmonic_mean")) {
            for (Map.Entry<String, Float> e : fuse(technique).entrySet()) {
                assertTrue(technique + " -> " + e.getKey() + " = " + e.getValue(), Float.isFinite(e.getValue()));
            }
        }
    }
}
