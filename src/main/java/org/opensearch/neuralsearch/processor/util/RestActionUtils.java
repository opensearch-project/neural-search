/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import org.opensearch.rest.RestRequest;

import java.util.Optional;

public class RestActionUtils {
    public static Optional<String[]> splitCommaSeparatedParam(RestRequest request, String paramName) {
        return Optional.ofNullable(request.param(paramName)).map(s -> s.split(","));
    }

    public static Optional<String> getStringParam(RestRequest request, String paramName) {
        return Optional.ofNullable(request.param(paramName));
    }
}
