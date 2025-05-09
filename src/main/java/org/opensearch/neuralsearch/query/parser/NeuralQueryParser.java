/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.parser;

import lombok.NonNull;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class NeuralQueryParser {
    /**
     * @param in stream input
     * @return vector supplier if it exists
     * @throws IOException
     */
    public static Supplier<float[]> vectorSupplierStreamInput(@NonNull final StreamInput in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        } else {
            final float[] vector = in.readFloatArray();
            return () -> vector;
        }
    }

    /**
     * Write the vector to the stream if it exists.
     * @param out output stream
     * @param vectorSupplier vector supplier
     * @throws IOException
     */
    public static void vectorSupplierStreamOutput(@NonNull final StreamOutput out, final Supplier<float[]> vectorSupplier)
        throws IOException {
        if (vectorSupplier == null || vectorSupplier.get() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeFloatArray(vectorSupplier.get());
        }
    }

    /**
     * @param in stream input
     * @return modelIdToVectorSupplierMap if it exists
     * @throws IOException
     */
    public static Map<String, Supplier<float[]>> modelIdToVectorSupplierMapStreamInput(@NonNull final StreamInput in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        } else {
            final Map<String, Supplier<float[]>> modelIdToVectorSupplierMap = new HashMap<>();
            final int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                final String modelId = in.readString();
                final float[] vector = in.readFloatArray();
                final Supplier<float[]> vectorSupplier = () -> vector;
                modelIdToVectorSupplierMap.put(modelId, vectorSupplier);
            }
            return modelIdToVectorSupplierMap;
        }
    }

    /**
     * Write the model id to vector map to the stream if all the vector suppliers have a value. All the vector suppliers
     * have values means all the async inference API calls are done it's safe to copy over the values to create the query
     * in another node. Otherwise, we should keep modelIdToVectorSupplierMap as null to trigger the inference calls
     * after the query is passed to another node.
     * @param out stream output
     * @param modelIdToVectorSupplierMap model id to vector supplier map
     * @throws IOException
     */
    public static void modelIdToVectorSupplierMapStreamOutput(
        @NonNull final StreamOutput out,
        final Map<String, Supplier<float[]>> modelIdToVectorSupplierMap
    ) throws IOException {
        if (modelIdToVectorSupplierMap == null) {
            out.writeBoolean(false);
            return;
        }
        boolean availableToWrite = true;
        for (Supplier<float[]> vectorSupplier : modelIdToVectorSupplierMap.values()) {
            if (vectorSupplier == null || vectorSupplier.get() == null) {
                availableToWrite = false;
            }
        }
        if (!availableToWrite) {
            out.writeBoolean(false);
            return;
        }

        int size = modelIdToVectorSupplierMap.size();
        out.writeInt(size);

        for (Map.Entry<String, Supplier<float[]>> entry : modelIdToVectorSupplierMap.entrySet()) {
            out.writeString(entry.getKey());
            out.writeFloatArray(entry.getValue().get());
        }
    }

    /**
     * @param in stream input
     * @return queryTokensMapSupplier if it exists
     * @throws IOException
     */
    public static Supplier<Map<String, Float>> queryTokensMapSupplierStreamInput(@NonNull final StreamInput in) throws IOException {
        if (in.readBoolean()) {
            final Map<String, Float> queryTokens = in.readMap(StreamInput::readString, StreamInput::readFloat);
            return () -> queryTokens;
        }
        return null;
    }

    /**
     * Write it to the stream if it exists.
     * @param out stream output
     * @param queryTokensMapSupplier query token map supplier for the sparse model
     * @throws IOException
     */
    public static void queryTokensMapSupplierStreamOutput(
        @NonNull final StreamOutput out,
        final Supplier<Map<String, Float>> queryTokensMapSupplier
    ) throws IOException {
        if (queryTokensMapSupplier == null || queryTokensMapSupplier.get() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(queryTokensMapSupplier.get(), StreamOutput::writeString, StreamOutput::writeFloat);
        }
    }

    public static Map<String, Supplier<Map<String, Float>>> modelIdToQueryTokensSupplierMapStreamInput(@NonNull final StreamInput in)
        throws IOException {
        if (!in.readBoolean()) {
            return null;
        } else {
            final Map<String, Supplier<Map<String, Float>>> modelIdToQueryTokensSupplierMap = new HashMap<>();
            final int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                final String modelId = in.readString();
                final Map<String, Float> queryTokenMap = in.readMap(StreamInput::readString, StreamInput::readFloat);
                final Supplier<Map<String, Float>> queryTokenMapSupplier = () -> queryTokenMap;
                modelIdToQueryTokensSupplierMap.put(modelId, queryTokenMapSupplier);
            }
            return modelIdToQueryTokensSupplierMap;
        }
    }

    /**
     * Write the model id to query token map to the stream if all the query token suppliers have a value. All the query
     * token suppliers have values means all the async inference API calls are done it's safe to copy over the values
     * to create the query in another node. Otherwise, we should keep modelIdToVectorSupplierMap as null to trigger
     * the inference calls after the query is passed to another node.
     * @param out stream output
     * @param modelIdToQueryTokensMapSupplier model id to query token map
     * @throws IOException
     */
    public static void modelIdToQueryTokensSupplierMapStreamOutput(
        @NonNull final StreamOutput out,
        final Map<String, Supplier<Map<String, Float>>> modelIdToQueryTokensMapSupplier
    ) throws IOException {
        if (modelIdToQueryTokensMapSupplier == null) {
            out.writeBoolean(false);
            return;
        }
        boolean availableToWrite = true;
        for (Supplier<Map<String, Float>> queryTokenMapSupplier : modelIdToQueryTokensMapSupplier.values()) {
            if (queryTokenMapSupplier == null || queryTokenMapSupplier.get() == null) {
                availableToWrite = false;
            }
        }
        if (!availableToWrite) {
            out.writeBoolean(false);
            return;
        }

        int size = modelIdToQueryTokensMapSupplier.size();
        out.writeInt(size);

        for (Map.Entry<String, Supplier<Map<String, Float>>> entry : modelIdToQueryTokensMapSupplier.entrySet()) {
            out.writeString(entry.getKey());
            out.writeMap(entry.getValue().get(), StreamOutput::writeString, StreamOutput::writeFloat);
        }
    }
}
