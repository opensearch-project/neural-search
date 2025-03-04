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

public class CountableStateStatSnapshotTests extends OpenSearchTestCase {
    private static final StateStatName STAT_NAME = StateStatName.TEXT_EMBEDDING_PROCESSORS;

    public void test_increment() {
        CountableStateStatSnapshot snapshot = new CountableStateStatSnapshot(STAT_NAME);
        assertEquals(0L, snapshot.getValue().longValue());
        snapshot.incrementBy(5L);
        assertEquals(5L, snapshot.getValue().longValue());
        snapshot.incrementBy(3L);
        assertEquals(8L, snapshot.getValue().longValue());
    }

    public void test_toXContent() throws IOException {
        CountableStateStatSnapshot snapshot = new CountableStateStatSnapshot(STAT_NAME);
        snapshot.incrementBy(42L);

        XContentBuilder builder = JsonXContent.contentBuilder();
        snapshot.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String jsonStr = builder.toString();

        // Verify JSON
        String expectedValueJson = String.format("\"%s\":42", StatSnapshot.VALUE_FIELD);
        String expectedTypeJson = String.format("\"%s\":\"%s\"", StatSnapshot.STAT_TYPE_FIELD, STAT_NAME.getStatType().getName());

        assertTrue(jsonStr.contains(expectedValueJson));
        assertTrue(jsonStr.contains(expectedTypeJson));
    }
}
