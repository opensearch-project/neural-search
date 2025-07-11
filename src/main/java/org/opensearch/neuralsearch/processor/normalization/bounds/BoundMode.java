/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

public enum BoundMode {
    APPLY,
    CLIP,
    IGNORE;

    public static final BoundMode DEFAULT = APPLY;

    public static String getValidValues() {
        return Arrays.stream(values()).map(mode -> mode.name().toLowerCase(Locale.ROOT)).collect(Collectors.joining(", "));
    }

    public static BoundMode fromString(String value) {
        if (Objects.isNull(value) || value.trim().isEmpty()) {
            return DEFAULT;
        }
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "invalid mode: %s, valid values are: %s", value, getValidValues())
            );
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
