/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.rankdocs;

/**
 * Fused document identity: a Lucene-local {@code docId}, its fused {@code score}, and the {@code shardIndex} it
 * belongs to. Deliberately a plain value — <b>not</b> {@code Writeable} — so no feature's wire-identity contract is
 * baked into a resolver-authored type. It is a candidate for the retriever framework to lift into OpenSearch core when
 * that framework's orchestration lands there; until then it lives resolver-locally.
 *
 * <p>Populated only on the {@code (shardIndex, docId)} fast path (PIT held). The default {@code _id}-keyed self-erase
 * path ({@link org.opensearch.neuralsearch.query.HybridFusionQuery}) does not use it.
 */
public record FusedDoc(int docId, float score, int shardIndex) {
}
