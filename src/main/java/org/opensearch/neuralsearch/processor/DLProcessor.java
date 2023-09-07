/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public abstract class DLProcessor extends AbstractProcessor {


    protected DLProcessor(String tag, String description) {
        super(tag, description);
    }
}
