/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.util.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.util.Optional;

@AllArgsConstructor
@Builder
@Getter
public class HybridQueryWithDLSRulesDTO {
    private final BooleanQuery firstClauseWithBooleanQuery;
    @Builder.Default
    private final Optional<Query> query = Optional.empty();
}
