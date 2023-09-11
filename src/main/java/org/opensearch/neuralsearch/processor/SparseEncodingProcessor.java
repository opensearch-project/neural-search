/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;


@Log4j2
public class SparseEncodingProcessor extends NLPProcessor {

    public static final String TYPE = "sparse_encoding";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";

    public SparseEncodingProcessor(String tag, String description, String modelId, Map<String, Object> fieldMap, MLCommonsClientAccessor clientAccessor, Environment environment) {
        super(tag, description, modelId, fieldMap, clientAccessor, environment);
        this.LIST_TYPE_NESTED_MAP_KEY =  "sparseEncoding";
    }

    @Override
    public void doExecute(IngestDocument ingestDocument, Map<String, Object> ProcessMap, List<String> inferenceList, BiConsumer<IngestDocument, Exception> handler) {
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(this.modelId, inferenceList, ActionListener.wrap(resultMaps -> {
            List<Map<String, ?> > results = new ArrayList<>();
            for (Map<String, ?> map: resultMaps)
            {
                results.addAll((List<Map<String, ?>>)map.get("response") );
            }
            setVectorFieldsToDocument(ingestDocument, ProcessMap, results);
            handler.accept(ingestDocument, null);
        }, e -> { handler.accept(null, e); }));
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
