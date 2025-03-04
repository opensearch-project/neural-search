/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class SettableStateStatSnapshotTests extends OpenSearchTestCase {

    private static final StateStatName STAT_NAME = StateStatName.CLUSTER_VERSION;
    private static final String SETTABLE_VALUE = "test-value";

    public void test_constructorWithoutValue() {
        SettableStateStatSnapshot<String> snapshot = new SettableStateStatSnapshot<>(STAT_NAME);
        assertNull(snapshot.getValue());
    }

    public void test_constructorWithValue() {
        SettableStateStatSnapshot<String> snapshot = new SettableStateStatSnapshot<>(STAT_NAME, SETTABLE_VALUE);
        assertEquals(SETTABLE_VALUE, snapshot.getValue());
    }

    public void test_setValueUpdates() {
        SettableStateStatSnapshot<String> snapshot = new SettableStateStatSnapshot<>(STAT_NAME);
        snapshot.setValue("new-value");
        assertEquals("new-value", snapshot.getValue());
    }

    public void test_toXContent() throws IOException {
        SettableStateStatSnapshot<String> snapshot = new SettableStateStatSnapshot<>(STAT_NAME, SETTABLE_VALUE);

        XContentBuilder builder = JsonXContent.contentBuilder();

        snapshot.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        // Verify the JSON structure
        String expectedValueJson = String.format("\"%s\":\"%s\"", StatSnapshot.VALUE_FIELD, SETTABLE_VALUE);
        String expectedTypeJson = String.format("\"%s\":\"%s\"", StatSnapshot.STAT_TYPE_FIELD, STAT_NAME.getStatType().getName());

        assertTrue(json.contains(expectedValueJson));
        assertTrue(json.contains(expectedTypeJson));
    }
}
