/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark tests that require a remote model server (e.g., TorchServe).
 * Tests marked with this annotation will only run when the remoteModelIntegTest task is executed.
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RequiresRemoteModel {
    /**
     * The type of remote model server required (e.g., "torchserve", "sagemaker", "bedrock")
     * @return the model server type
     */
    String value() default "torchserve";

    /**
     * The specific model name required for the test
     * @return the model name
     */
    String model() default "semantic_highlighter";

    /**
     * Optional endpoint override for the model server
     * @return the endpoint URL
     */
    String endpoint() default "";
}
