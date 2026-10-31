/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.profile;

import java.util.List;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;

public class FastPathDecisionTests extends OpenSearchTestCase {

    public void testToMap_whenNothingRefused_thenWouldTakeAndOnlyEvaluatedFactsRender() {
        FastPathDecision decision = new FastPathDecision().fetchEstimateBytes(600_000L).fetchBudgetBytes(1_048_576L).countSettled(true);

        Map<String, Object> map = decision.toMap();

        assertTrue(decision.allowsSoFar());
        assertEquals(List.of("would_take", "fetch_estimate_bytes", "fetch_budget_bytes", "count_settled"), List.copyOf(map.keySet()));
        assertEquals(true, map.get("would_take"));
        assertEquals(600_000L, map.get("fetch_estimate_bytes"));
        assertEquals(1_048_576L, map.get("fetch_budget_bytes"));
        assertEquals(true, map.get("count_settled"));
    }

    public void testToMap_whenRefused_thenReasonAndDetailRenderAndUnevaluatedFactsDoNot() {
        FastPathDecision decision = new FastPathDecision().refuse(FastPathDecision.LEG_NAME, "leg 1 carries _name");

        Map<String, Object> map = decision.toMap();

        assertFalse(decision.allowsSoFar());
        assertEquals(List.of("would_take", "refused_by", "detail"), List.copyOf(map.keySet()));
        assertEquals(false, map.get("would_take"));
        assertEquals("leg_name", map.get("refused_by"));
        assertEquals("leg 1 carries _name", map.get("detail"));
    }

    public void testRefuse_whenCalledTwice_thenTheFirstReasonStands() {
        FastPathDecision decision = new FastPathDecision().refuse(FastPathDecision.FETCH_BUDGET, "over budget")
            .refuse(FastPathDecision.COUNT_NOT_SETTLED, "unsettled");

        assertEquals(FastPathDecision.FETCH_BUDGET, decision.refusedBy());
        assertEquals("over budget", decision.detail());
    }

    public void testToMap_whenRefusedWithoutDetail_thenDetailIsOmitted() {
        Map<String, Object> map = new FastPathDecision().refuse(FastPathDecision.NO_CANDIDATES, null).toMap();

        assertEquals(List.of("would_take", "refused_by"), List.copyOf(map.keySet()));
    }
}
