/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* This class is built based on lucene FeatureQuery. We use LinearFuntion and add an upperbound to it */

package org.opensearch.neuralsearch.query.sparse;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ImpactsDISI;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;

public final class BoundedLinearFeatureQuery extends Query {

    private final String fieldName;
    private final String featureName;
    private final Float scoreUpperBound;

    public BoundedLinearFeatureQuery(String fieldName, String featureName, Float scoreUpperBound) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.featureName = Objects.requireNonNull(featureName);
        this.scoreUpperBound = Objects.requireNonNull(scoreUpperBound);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        return super.rewrite(indexSearcher);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BoundedLinearFeatureQuery that = (BoundedLinearFeatureQuery) obj;
        return Objects.equals(fieldName, that.fieldName)
                && Objects.equals(featureName, that.featureName)
                && Objects.equals(scoreUpperBound, that.scoreUpperBound);
    }

    @Override
    public int hashCode() {
        int h = getClass().hashCode();
        h = 31 * h + fieldName.hashCode();
        h = 31 * h + featureName.hashCode();
        h = 31 * h + scoreUpperBound.hashCode();
        return h;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
            throws IOException {
        if (!scoreMode.needsScores()) {
            // We don't need scores (e.g. for faceting), and since features are stored as terms,
            // allow TermQuery to optimize in this case
            TermQuery tq = new TermQuery(new Term(fieldName, featureName));
            return searcher.rewrite(tq).createWeight(searcher, scoreMode, boost);
        }

        return new Weight(this) {

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                String desc = "weight(" + getQuery() + " in " + doc + ") [\" BoundedLinearFeatureQuery \"]";

                Terms terms = context.reader().terms(fieldName);
                if (terms == null) {
                    return Explanation.noMatch(desc + ". Field " + fieldName + " doesn't exist.");
                }
                TermsEnum termsEnum = terms.iterator();
                if (termsEnum.seekExact(new BytesRef(featureName)) == false) {
                    return Explanation.noMatch(desc + ". Feature " + featureName + " doesn't exist.");
                }

                PostingsEnum postings = termsEnum.postings(null, PostingsEnum.FREQS);
                if (postings.advance(doc) != doc) {
                    return Explanation.noMatch(desc + ". Feature " + featureName + " isn't set.");
                }

                int freq = postings.freq();
                float featureValue = decodeFeatureValue(freq);
                float score = boost * featureValue;
                return Explanation.match(
                        score,
                        "Linear function on the "
                                + fieldName
                                + " field for the "
                                + featureName
                                + " feature, computed as w * S from:",
                        Explanation.match(boost, "w, weight of this function"),
                        Explanation.match(featureValue, "S, feature value"));
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                Terms terms = Terms.getTerms(context.reader(), fieldName);
                TermsEnum termsEnum = terms.iterator();
                if (termsEnum.seekExact(new BytesRef(featureName)) == false) {
                    return null;
                }

                final SimScorer scorer = new SimScorer() {
                    @Override
                    public float score(float freq, long norm) {
                        return boost * decodeFeatureValue(freq);
                    }
                };
                final ImpactsEnum impacts = termsEnum.impacts(PostingsEnum.FREQS);
                final ImpactsDISI impactsDisi = new ImpactsDISI(impacts, impacts, scorer);

                return new Scorer(this) {

                    @Override
                    public int docID() {
                        return impacts.docID();
                    }

                    @Override
                    public float score() throws IOException {
                        return scorer.score(impacts.freq(), 1L);
                    }

                    @Override
                    public DocIdSetIterator iterator() {
                        return impactsDisi;
                    }

                    @Override
                    public int advanceShallow(int target) throws IOException {
                        return impactsDisi.advanceShallow(target);
                    }

                    @Override
                    public float getMaxScore(int upTo) throws IOException {
                        return impactsDisi.getMaxScore(upTo);
                    }

                    @Override
                    public void setMinCompetitiveScore(float minScore) {
                        impactsDisi.setMinCompetitiveScore(minScore);
                    }
                };
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String field) {
        return "BoundedLinearFeatureQuery(field="
                + fieldName
                + ", feature="
                + featureName
                + ", scoreUpperBound="
                + scoreUpperBound
                + ")";
    }

    static final int MAX_FREQ = Float.floatToIntBits(Float.MAX_VALUE) >>> 15;
    private float decodeFeatureValue(float freq) {
        if (freq > MAX_FREQ) {
            return scoreUpperBound;
        }
        int tf = (int) freq; // lossless
        int featureBits = tf << 15;
        return Math.min(Float.intBitsToFloat(featureBits), scoreUpperBound);
    }
}