/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.grouping.GroupSelector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.fielddata.AbstractSortedDocValues;

import java.io.IOException;
import java.util.Collection;

public class CollapseDocSourceGroupSelector<T extends org.apache.lucene.util.BytesRef> extends GroupSelector<
    org.apache.lucene.util.BytesRef> {

    private final String field;
    private SortedDocValues values;
    private int ord;

    public CollapseDocSourceGroupSelector(String field) {
        this.field = field;
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
        LeafReader reader = readerContext.reader();
        DocValuesType type = getDocValuesType(reader, this.field);
        if (type != null && type != DocValuesType.NONE) {
            switch (type) {
                case SORTED:
                    this.values = DocValues.getSorted(reader, this.field);
                    break;
                case SORTED_SET:
                    final SortedSetDocValues sorted = DocValues.getSortedSet(reader, this.field);
                    this.values = DocValues.unwrapSingleton(sorted);
                    if (this.values == null) {
                        this.values = new AbstractSortedDocValues() {
                            private int ord;

                            public boolean advanceExact(int target) throws IOException {
                                if (sorted.advanceExact(target)) {
                                    this.ord = (int) sorted.nextOrd();
                                    if (sorted.docValueCount() != 1) {
                                        throw new IllegalStateException(
                                            "failed to collapse " + target + ", the collapse field must be single valued"
                                        );
                                    } else {
                                        return true;
                                    }
                                } else {
                                    return false;
                                }
                            }

                            public int docID() {
                                return sorted.docID();
                            }

                            public int ordValue() {
                                return this.ord;
                            }

                            public BytesRef lookupOrd(int ord) throws IOException {
                                return sorted.lookupOrd((long) ord);
                            }

                            public int getValueCount() {
                                return (int) sorted.getValueCount();
                            }
                        };
                    }
                    break;
                default:
                    String var10002 = String.valueOf(type);
                    throw new IllegalStateException("unexpected doc values type " + var10002 + "` for field `" + this.field + "`");
            }

        } else {
            this.values = DocValues.emptySorted();
        }
    }

    @Override
    public void setScorer(Scorable scorable) throws IOException {

    }

    @Override
    public State advanceTo(int i) throws IOException {
        if (this.values.advanceExact(i)) {
            this.ord = this.values.ordValue();
            return State.ACCEPT;
        } else {
            this.ord = -1;
            return State.SKIP;
        }
    }

    @Override
    public org.apache.lucene.util.BytesRef currentValue() throws IOException {
        if (this.ord == -1) {
            return null;
        } else {
            try {
                return this.values.lookupOrd(this.ord);
            } catch (IOException var2) {
                IOException e = var2;
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public BytesRef copyValue() throws IOException {
        BytesRef value = this.currentValue();
        return value == null ? null : BytesRef.deepCopyOf(value);
    }

    @Override
    public void setGroups(Collection<SearchGroup<BytesRef>> collection) {
        throw new UnsupportedOperationException();
    }

    private static DocValuesType getDocValuesType(LeafReader in, String field) {
        FieldInfo fi = in.getFieldInfos().fieldInfo(field);
        return fi != null ? fi.getDocValuesType() : null;
    }
}
