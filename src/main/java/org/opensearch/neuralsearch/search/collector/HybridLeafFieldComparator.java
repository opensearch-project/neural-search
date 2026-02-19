/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.Getter;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;

import java.io.IOException;

/**
 * A wrapper for {@link LeafFieldComparator} that intercepts scorer calls to return individual
 * sub-query scores instead of the sum of all sub-query scores.
 *
 * <p>In hybrid queries, the {@link HybridSubQueryScorer#score()} method returns the sum of all
 * sub-query scores. However, when collecting documents per sub-query (e.g., in collapse scenarios),
 * we need to sort documents within each sub-query based on that sub-query's individual score,
 * not the total score across all sub-queries.
 *
 * <p>This wrapper solves the problem by:
 * <ol>
 *   <li>Storing the current sub-query's individual score via {@link #setCurrentSubQueryScore(float)}</li>
 *   <li>Wrapping the scorer to return this individual score instead of the sum</li>
 *   <li>Delegating all other comparator operations to the underlying comparator</li>
 * </ol>
 *
 * <h3>Example Usage:</h3>
 * <pre>{@code
 * // When collecting a document for sub-query 0 with score 0.9
 * HybridLeafFieldComparator wrapper = new HybridLeafFieldComparator(actualComparator);
 * wrapper.setScorer(hybridSubQueryScorer);  // hybridSubQueryScorer.score() returns 1.4 (sum)
 * wrapper.setCurrentSubQueryScore(0.9);     // Set individual score for sub-query 0
 * wrapper.copy(slot, doc);                  // Copies 0.9 (not 1.4) to the comparator
 * }</pre>
 *
 * <h3>Why This is Needed:</h3>
 * <p>Consider a hybrid query with two sub-queries where a document matches both:
 * <ul>
 *   <li>Sub-query 0 score: 0.9</li>
 *   <li>Sub-query 1 score: 0.5</li>
 *   <li>Total score (sum): 1.4</li>
 * </ul>
 *
 * <p>Without this wrapper, when sorting documents for sub-query 0, the comparator would use
 * the total score (1.4) instead of the individual score (0.9), causing incorrect ranking within
 * that sub-query's results.
 *
 * @see HybridSubQueryScorer
 * @see HybridCollapsingTopDocsCollector
 */
public class HybridLeafFieldComparator implements LeafFieldComparator {
    private final LeafFieldComparator delegate;
    @Getter
    private float currentSubQueryScore;

    /**
     * Creates a new wrapper around the given comparator.
     *
     * @param delegate The underlying comparator to delegate operations to
     */
    public HybridLeafFieldComparator(LeafFieldComparator delegate) {
        this.delegate = delegate;
    }

    /**
     * Sets the individual sub-query score to be returned by the wrapped scorer.
     * This must be called before {@link #copy(int, int)} to ensure the correct
     * score is stored in the comparator.
     *
     * @param score The individual sub-query score for the current document
     */
    public void setCurrentSubQueryScore(float score) {
        this.currentSubQueryScore = score;
    }

    /**
     * Sets the scorer on the delegate comparator, wrapping it to return the
     * individual sub-query score instead of the sum of all sub-query scores.
     *
     * @param scorer The scorer to wrap (typically a {@link HybridSubQueryScorer})
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void setScorer(Scorable scorer) throws IOException {
        // Wrap the HybridSubQueryScorer to return individual score instead of sum
        Scorable wrappedScorer = new Scorable() {
            @Override
            public float score() throws IOException {
                return currentSubQueryScore;  // Returns individual sub-query score
            }
        };

        delegate.setScorer(wrappedScorer);
    }

    /**
     * Copies the current document's field values to the specified slot.
     * The wrapped scorer ensures the individual sub-query score is used.
     *
     * @param slot The slot to copy values to
     * @param doc The document ID to copy from
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void copy(int slot, int doc) throws IOException {
        delegate.copy(slot, doc);
    }

    @Override
    public void setBottom(int slot) throws IOException {
        delegate.setBottom(slot);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        return delegate.compareBottom(doc);
    }

    @Override
    public int compareTop(int doc) throws IOException {
        return delegate.compareTop(doc);
    }
}
