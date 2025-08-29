/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

public class RamBytesRecorderTests extends AbstractSparseTestBase {

    public void testConstructorWithChecker() {
        BiPredicate<Long, Long> checker = (increment, total) -> total <= 100;
        RamBytesRecorder recorder = new RamBytesRecorder(checker);
        assertEquals(0L, recorder.getBytes());
    }

    public void testConstructorWithInitialBytes() {
        RamBytesRecorder recorder = new RamBytesRecorder(50L);
        assertEquals(50L, recorder.getBytes());
    }

    public void testRecordWithoutChecker() {
        RamBytesRecorder recorder = new RamBytesRecorder(0L);
        assertTrue(recorder.record(10L));
        assertEquals(10L, recorder.getBytes());
        assertTrue(recorder.record(-5L));
        assertEquals(5L, recorder.getBytes());
    }

    public void testRecordWithCheckerSuccess() {
        BiPredicate<Long, Long> checker = (increment, total) -> total <= 100;
        RamBytesRecorder recorder = new RamBytesRecorder(checker);
        assertTrue(recorder.record(50L));
        assertEquals(50L, recorder.getBytes());
    }

    public void testRecordWithCheckerFailure() {
        BiPredicate<Long, Long> checker = (increment, total) -> total <= 100;
        RamBytesRecorder recorder = new RamBytesRecorder(checker);
        recorder.record(90L);
        assertFalse(recorder.record(20L));
        assertEquals(90L, recorder.getBytes());
    }

    public void testRecordNegativeWithChecker() {
        BiPredicate<Long, Long> checker = (increment, total) -> total <= 100;
        RamBytesRecorder recorder = new RamBytesRecorder(checker);
        recorder.record(50L);
        assertTrue(recorder.record(-10L));
        assertEquals(40L, recorder.getBytes());
    }

    public void testSafeRecordWithoutAction() {
        RamBytesRecorder recorder = new RamBytesRecorder(0L);
        recorder.safeRecord(25L, null);
        assertEquals(25L, recorder.getBytes());
    }

    public void testSafeRecordWithAction() {
        RamBytesRecorder recorder = new RamBytesRecorder(0L);
        AtomicLong actionValue = new AtomicLong(0L);
        Consumer<Long> action = actionValue::set;
        recorder.safeRecord(30L, action);
        assertEquals(30L, recorder.getBytes());
        assertEquals(30L, actionValue.get());
    }

    public void testSetCanRecordIncrementChecker() {
        RamBytesRecorder recorder = new RamBytesRecorder(0L);
        BiPredicate<Long, Long> checker = (increment, total) -> total <= 50;
        recorder.setCanRecordIncrementChecker(checker);
        assertTrue(recorder.record(30L));
        assertFalse(recorder.record(30L));
        assertEquals(30L, recorder.getBytes());
    }
}
