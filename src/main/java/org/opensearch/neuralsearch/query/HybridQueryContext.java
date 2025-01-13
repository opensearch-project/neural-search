/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Builder;
import lombok.Getter;

/**
 * Class that holds the low level information of hybrid query in the form of context
 */
@Builder
@Getter
public class HybridQueryContext {
    private int paginationDepth;
}
