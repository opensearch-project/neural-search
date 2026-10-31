/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.lucene.search.Query;
import org.opensearch.common.SetOnce;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.transport.client.Client;

/**
 * A test leg that rewrites the way a {@code neural} query does on the coordinator, with the model call made countable.
 *
 * <p>First rewrite: register an async action standing for the inference and return a <b>new</b> builder holding the
 * {@link SetOnce} the action fills — the user's original keeps a {@code null} supplier, exactly as
 * {@code NeuralQueryBuilder#rewriteQueryAgainstKnnField} leaves it. Next rewrite, once the action has run: return the
 * resolved form, a {@link TermQueryBuilder} on {@link #field} for {@link #text} standing in for the {@code neural_knn} a real
 * leg becomes. Every rewrite of a builder whose supplier is still {@code null} registers another "inference", which is the
 * defect this fake exists to count: {@link #inferences} says how many times the model would have been called.
 *
 * <p>The async actions can be held back ({@link #holdActions}) so a test can complete them in an order of its choosing, or
 * made to fail ({@link #failWith}) to see how a leg failure is reported. Never serialized and never converted to a Lucene
 * query: it lives on the coordinator, like the rewrite it imitates.
 */
final class ResolvingLegQueryBuilder extends AbstractQueryBuilder<ResolvingLegQueryBuilder> {

    static final String NAME = "resolving_leg";

    /** What a resolved leg becomes: a term query on this field. */
    private final String field;
    private final String text;
    private final Supplier<String> resolvedText;
    private final Shared shared;

    /** State shared by a builder and the copies its rewrites produce, so a test holds one handle to all of them. */
    static final class Shared {
        /** How many model calls were registered across every rewrite of this leg and its copies. */
        final AtomicInteger inferences = new AtomicInteger();
        /** Held-back completions, in registration order, when {@link #holdActions} is set. */
        final List<Runnable> pending = new CopyOnWriteArrayList<>();
        volatile boolean holdActions;
        volatile Exception failWith;
    }

    ResolvingLegQueryBuilder(final String field, final String text) {
        this(field, text, null, new Shared());
    }

    private ResolvingLegQueryBuilder(final String field, final String text, final Supplier<String> resolvedText, final Shared shared) {
        this.field = field;
        this.text = text;
        this.resolvedText = resolvedText;
        this.shared = shared;
    }

    /** The handle a test counts and steers the leg's model calls through. */
    Shared shared() {
        return shared;
    }

    /** How many times the model would have been called for this leg, across all of its rewritten copies. */
    int inferences() {
        return shared.inferences.get();
    }

    /** Hold every registered action until {@link #completePending()} releases it. */
    ResolvingLegQueryBuilder holdActions() {
        shared.holdActions = true;
        return this;
    }

    /** Make every registered action fail with {@code failure} instead of resolving. */
    ResolvingLegQueryBuilder failWith(final Exception failure) {
        shared.failWith = failure;
        return this;
    }

    /** Release the held-back actions, oldest first. */
    void completePending() {
        List<Runnable> toRun = List.copyOf(shared.pending);
        shared.pending.clear();
        toRun.forEach(Runnable::run);
    }

    /** The resolved form this leg rewrites into once its "inference" has answered. */
    TermQueryBuilder resolvedForm() {
        return new TermQueryBuilder(field, text);
    }

    @Override
    protected QueryBuilder doRewrite(final QueryRewriteContext queryRewriteContext) {
        if (Objects.nonNull(resolvedText)) {
            if (Objects.isNull(resolvedText.get())) {
                return this;
            }
            return new TermQueryBuilder(field, resolvedText.get());
        }
        SetOnce<String> resolved = new SetOnce<>();
        shared.inferences.incrementAndGet();
        BiConsumer<Client, ActionListener<?>> inference = (client, listener) -> {
            Runnable complete = () -> {
                if (Objects.nonNull(shared.failWith)) {
                    listener.onFailure(shared.failWith);
                    return;
                }
                resolved.set(text);
                listener.onResponse(null);
            };
            if (shared.holdActions) {
                shared.pending.add(complete);
            } else {
                complete.run();
            }
        };
        queryRewriteContext.registerAsyncAction(inference);
        return new ResolvingLegQueryBuilder(field, text, resolved::get, shared);
    }

    @Override
    protected void doWriteTo(final StreamOutput out) {
        throw new UnsupportedOperationException("a resolving test leg lives on the coordinator and is never serialized");
    }

    @Override
    protected void doXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(field, text);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(final QueryShardContext context) {
        throw new UnsupportedOperationException("a resolving test leg is rewritten away before any shard sees it");
    }

    @Override
    protected boolean doEquals(final ResolvingLegQueryBuilder other) {
        return Objects.equals(field, other.field) && Objects.equals(text, other.text);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, text);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
