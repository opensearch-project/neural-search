/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingExecutor;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Collections;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseVectorField.SPARSE_FIELD;

/**
 * The native writer is the one path that outlives the ingest gate: a background merge of segments
 * written while the engine was on would otherwise reach the JNI library. These tests assert it is
 * skipped without touching {@link org.opensearch.neuralsearch.jni.NativeLibrary}, whose static
 * initializer loads the shared object.
 */
public class NativeDocValuesConsumerTests extends AbstractSparseTestBase {
    @Mock
    private MergeHelper mergeHelper;
    @Mock
    private MergeStateFacade mergeStateFacade;
    @Mock
    private FieldInfo nativeFieldInfo;
    @Mock
    private FieldInfo luceneFieldInfo;
    @Mock
    private SegmentInfo segmentInfo;

    private DocValuesProducer valuesProducer;
    private NativeDocValuesConsumer consumer;

    @SneakyThrows
    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        Map<String, String> nativeAttributes = new HashMap<>();
        nativeAttributes.put(SPARSE_FIELD, String.valueOf(true));
        nativeAttributes.put(ENGINE_FIELD, SparseEngine.NATIVE.getName());
        when(nativeFieldInfo.attributes()).thenReturn(nativeAttributes);
        when(nativeFieldInfo.getAttribute(ENGINE_FIELD)).thenReturn(SparseEngine.NATIVE.getName());
        when(nativeFieldInfo.getName()).thenReturn("native_field");

        Map<String, String> luceneAttributes = new HashMap<>();
        luceneAttributes.put(SPARSE_FIELD, String.valueOf(true));
        luceneAttributes.put(ENGINE_FIELD, SparseEngine.LUCENE.getName());
        when(luceneFieldInfo.attributes()).thenReturn(luceneAttributes);
        when(luceneFieldInfo.getAttribute(ENGINE_FIELD)).thenReturn(SparseEngine.LUCENE.getName());
        when(luceneFieldInfo.getName()).thenReturn("lucene_field");

        SegmentWriteState state = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        valuesProducer = mock(DocValuesProducer.class);
        consumer = new NativeDocValuesConsumer(state, mergeHelper);

        // Uninitialized settings means the dynamic gate sits at its default, off
        SparseSettings.reset();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        SparseSettings.reset();
        super.tearDown();
    }

    @SneakyThrows
    public void testAddBinaryFieldSkipsNativeFieldWhenDisabled() {
        consumer.addBinaryField(nativeFieldInfo, valuesProducer);

        // Never reads the values, so nothing reaches the native index writer
        verify(valuesProducer, never()).getBinary(any());
    }

    @SneakyThrows
    public void testMergeSkipsNativeFieldWhenDisabled() {
        consumer.merge(List.of(nativeFieldInfo), mergeStateFacade);

        verify(mergeHelper, never()).newSparseDocValuesReader(any());
    }

    @SneakyThrows
    public void testAddBinaryFieldIgnoresLuceneEngineField() {
        consumer.addBinaryField(luceneFieldInfo, valuesProducer);

        verify(valuesProducer, never()).getBinary(any());
    }

    @SneakyThrows
    public void testMergeIgnoresLuceneEngineField() {
        consumer.merge(List.of(luceneFieldInfo), mergeStateFacade);

        verify(mergeHelper, never()).newSparseDocValuesReader(any());
    }

    // ---- the write paths, with the engine gate open ----

    /**
     * With both gates open the consumer actually builds the index, so this runs the real library.
     * The assertion is that an engine file lands in the segment: the contents are covered by
     * NativeIndexRoundTripTests.
     */
    @SneakyThrows
    public void testAddBinaryFieldWritesTheEngineFileWhenEnabled() {
        NativeEngineFixture fixture = new NativeEngineFixture();
        try {
            DocValuesProducer producer = mock(DocValuesProducer.class);
            when(producer.getBinary(fixture.fieldInfo())).thenReturn(fixture.docValues());

            new NativeDocValuesConsumer(fixture.state(), mergeHelper).addBinaryField(fixture.fieldInfo(), producer);

            assertTrue(fixture.engineFileWritten());
        } finally {
            fixture.close();
        }
    }

    @SneakyThrows
    public void testMergeWritesTheEngineFileWhenEnabled() {
        NativeEngineFixture fixture = new NativeEngineFixture();
        try {
            SparseDocValuesReader reader = mock(SparseDocValuesReader.class);
            when(reader.getBinary(fixture.fieldInfo())).thenReturn(fixture.docValues());
            when(mergeHelper.newSparseDocValuesReader(mergeStateFacade)).thenReturn(reader);

            new NativeDocValuesConsumer(fixture.state(), mergeHelper).merge(List.of(fixture.fieldInfo()), mergeStateFacade);

            assertTrue(fixture.engineFileWritten());
        } finally {
            fixture.close();
        }
    }

    /** A real segment, directory and field with the native engine on, plus one document. */
    private static class NativeEngineFixture {
        private final Directory directory;
        private final SegmentInfo segment;
        private final FieldInfo field;

        @SneakyThrows
        NativeEngineFixture() {
            directory = FSDirectory.open(createTempDir());
            segment = new SegmentInfo(
                directory,
                Version.LATEST,
                Version.LATEST,
                "_0",
                1,
                false,
                false,
                Codec.getDefault(),
                Collections.emptyMap(),
                new byte[StringHelper.ID_LENGTH],
                Collections.emptyMap(),
                null
            );
            field = new FieldInfo(
                "native_field",
                0,
                false,
                false,
                false,
                IndexOptions.DOCS,
                DocValuesType.BINARY,
                DocValuesSkipIndexType.NONE,
                -1,
                new HashMap<>(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
            field.putAttribute(SPARSE_FIELD, "true");
            field.putAttribute(ENGINE_FIELD, SparseEngine.NATIVE.getName());
            // Above the doc count, so the writer builds an inverted index and skips clustering
            field.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(Integer.MAX_VALUE));

            Settings nodeSettings = Settings.builder()
                .put(SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED, true)
                .put(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED, true)
                .build();
            // Every node-scoped sparse setting the plugin registers, rather than the few this test
            // reads: the consumer consults whichever ones its writers need, and an unregistered one
            // throws rather than defaulting.
            ClusterSettings clusterSettings = new ClusterSettings(
                nodeSettings,
                SparseSettings.state()
                    .getSettings()
                    .stream()
                    .filter(setting -> setting.hasNodeScope())
                    .collect(java.util.stream.Collectors.toUnmodifiableSet())
            );
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.getSettings()).thenReturn(nodeSettings);
            when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
            ClusterTrainingExecutor.getInstance().initialize(mock(ThreadPool.class));
            SparseSettings.state().initialize(clusterService, nodeSettings);
        }

        FieldInfo fieldInfo() {
            return field;
        }

        SegmentWriteState state() {
            return new SegmentWriteState(
                InfoStream.getDefault(),
                directory,
                segment,
                new FieldInfos(new FieldInfo[] { field }),
                null,
                IOContext.DEFAULT
            );
        }

        @SneakyThrows
        boolean engineFileWritten() {
            String name = CodecUtils.buildIndexFileName(
                segment.name,
                SparseEngine.NATIVE.version(),
                field.getName(),
                SparseEngine.NATIVE.extension()
            );
            return Arrays.asList(directory.listAll()).contains(name);
        }

        /** One document holding a single token, enough to build an index from. */
        BinaryDocValues docValues() {
            return new BinaryDocValues() {
                private int doc = -1;

                @Override
                @SneakyThrows
                public BytesRef binaryValue() {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    try (DataOutputStream dos = new DataOutputStream(baos)) {
                        dos.writeInt(7);
                        dos.writeFloat(1.0f);
                    }
                    return new BytesRef(baos.toByteArray());
                }

                @Override
                public boolean advanceExact(int target) {
                    doc = target;
                    return true;
                }

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public int nextDoc() {
                    doc++;
                    return doc < 1 ? doc : NO_MORE_DOCS;
                }

                @Override
                public int advance(int target) {
                    doc = target;
                    return doc < 1 ? doc : NO_MORE_DOCS;
                }

                @Override
                public long cost() {
                    return 1;
                }
            };
        }

        @SneakyThrows
        void close() {
            directory.close();
            SparseSettings.reset();
        }
    }
}
