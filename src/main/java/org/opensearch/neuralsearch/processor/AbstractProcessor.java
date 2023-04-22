/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class AbstractProcessor {

    private String description;
    private String tag;

    public String getTag() {
        return tag;
    }

    public String getDescription() {
        return description;
    }

}
