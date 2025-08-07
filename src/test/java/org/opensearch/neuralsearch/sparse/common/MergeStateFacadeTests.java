/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.SneakyThrows;
import org.apache.lucene.index.MergeState;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

/**
 * Unit tests for {@link MergeStateFacade}
 */
public class MergeStateFacadeTests extends AbstractSparseTestBase {

    private MergeState mergeState;
    private MergeStateFacade mergeStateFacade;

    /**
     * Setup method to initialize mocks before each test
     */
    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        // Initialize mocks
        mergeState = TestsPrepareUtils.prepareMergeState(false);
        // Create the facade with the mock
        mergeStateFacade = new MergeStateFacade(mergeState);
    }

    /**
     * Test that getDocValuesProducers returns the correct DocValuesProducer array
     */
    public void testGetDocValuesProducers() {
        assertSame(mergeState.docValuesProducers, mergeStateFacade.getDocValuesProducers());
    }

    /**
     * Test that getMergeFieldInfos returns the correct FieldInfos
     */
    public void testGetMergeFieldInfos() {
        assertSame(mergeState.mergeFieldInfos, mergeStateFacade.getMergeFieldInfos());
    }

    /**
     * Test that getMaxDocs returns the correct maxDocs array
     */
    public void testGetMaxDocs() {
        assertSame(mergeState.maxDocs, mergeStateFacade.getMaxDocs());
    }

    /**
     * Test that getDocMaps returns the correct DocMap array
     */
    public void testGetDocMaps() {
        assertSame(mergeState.docMaps, mergeStateFacade.getDocMaps());
    }

    /**
     * Test that getLiveDocs returns the correct Bits array
     */
    public void testGetLiveDocs() {
        assertSame(mergeState.liveDocs, mergeStateFacade.getLiveDocs());
    }

    /**
     * Test constructor with null MergeState
     * Should throw NullPointerException
     */
    public void testConstructorWithNullMergeState() {
        assertThrows(NullPointerException.class, () -> new MergeStateFacade(null));
    }
}
