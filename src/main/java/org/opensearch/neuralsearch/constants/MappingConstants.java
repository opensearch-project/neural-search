/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.constants;

/**
 * Constants related to the index mapping.
 */
public class MappingConstants {
    /**
     * Name for the field type. In index mapping we use this key to define the field type.
     */
    public static final String TYPE = "type";
    /**
     * Name for doc. Actions like create index and legacy create/update index template will have the
     * mapping properties under a _doc key.
     */
    public static final String DOC = "_doc";
    /**
     * Name for properties. An object field will define subfields as properties.
     */
    public static final String PROPERTIES = "properties";

    /**
     * Separator in a field path.
     */
    public static final String PATH_SEPARATOR = ".";
}
