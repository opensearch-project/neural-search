## What/Why

### What are you proposing?

Introduce size-based (byte-aware) batching for neural search ingest processors (`TextEmbeddingProcessor`, `SparseEncodingProcessor`, and `SemanticFieldProcessor`) as an alternative to the current static document-count-based `batch_size`. Instead of batching a fixed number of documents regardless of their content size, the processor would accumulate documents into a batch until a configurable byte threshold (`batch_size_bytes`) is reached. This ensures that the actual payload sent to ML Commons for inference remains predictable and bounded, regardless of individual document sizes.

A hybrid mode is also proposed where both `batch_size` (max document count) and `batch_size_bytes` (max byte size) can be configured together — whichever limit is hit first triggers the batch to be dispatched.

### What users have asked for this feature?

- The original batch ingestion RFC ([neural-search#598](https://github.com/opensearch-project/neural-search/issues/598)) and the OpenSearch core RFC ([OpenSearch#12457](https://github.com/opensearch-project/OpenSearch/issues/12457)) both discuss batch size configuration but only in terms of document count. Community feedback on these RFCs has highlighted that static document counts do not account for payload variance.
- [neural-search#743](https://github.com/opensearch-project/neural-search/issues/743) implemented batch ingestion for `TextEmbeddingProcessor` and `SparseEncodingProcessor` with a static `batch_size`, and benchmarks showed 77% ingestion time reduction. However, users with heterogeneous document sizes report suboptimal batching — either hitting ML Commons memory limits with large documents or underutilizing capacity with small ones.
- The analogous problem is well-documented in the broader search ecosystem. For example, [Elasticsuite#3239](https://github.com/Smile-SA/elasticsuite/issues/3239) describes how batches exceeding size limits cause indexing failures, forcing users to either over-provision infrastructure or drastically reduce batch sizes.
- ML service providers (OpenAI, SageMaker, Bedrock, Cohere) impose rate limits based on tokens/bytes per request, not document count. A size-aware batching strategy aligns more naturally with these upstream constraints.

### What problems are you trying to solve?

- When **ingesting documents of varying sizes through a neural search pipeline**, a **user** wants **the batch size to adapt to the actual data volume** so they **don't hit ML Commons memory limits or remote model rate limits with oversized batches, and don't underutilize throughput with undersized batches**.

- When **bulk-indexing a corpus with heterogeneous document lengths** (e.g., a mix of short product titles and long article bodies), a **cluster administrator** wants **predictable and bounded inference request sizes** so they **can capacity-plan their ML nodes and remote model endpoints without needing to tune batch_size per index or document type**.

- When **using remote models with token-based or byte-based rate limits** (e.g., OpenAI, SageMaker, Bedrock), a **developer** wants **the ingest processor to respect payload size constraints automatically** so they **avoid throttling errors (HTTP 429) that cause ingestion failures and retries**.

- When **operating a multi-tenant cluster where different indices have different document profiles**, an **operations engineer** wants **a single batching configuration that works well across all indices** so they **don't need to maintain per-index pipeline configurations with different batch_size values**.

### What is the developer experience going to be?

The feature introduces a new optional parameter `batch_size_bytes` to neural search ingest processors. It works alongside the existing `batch_size` parameter.

**Pipeline Configuration (REST API):**

```json
PUT _ingest/pipeline/my-embedding-pipeline
{
  "processors": [
    {
      "text_embedding": {
        "model_id": "my-model-id",
        "field_map": {
          "passage_text": "passage_embedding"
        },
        "batch_size": 100,
        "batch_size_bytes": 102400
      }
    }
  ]
}
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `batch_size` | integer | Existing default (from `AbstractBatchingProcessor`) | Maximum number of documents per batch. Existing behavior, unchanged. |
| `batch_size_bytes` | integer | `-1` (disabled) | Maximum cumulative size in bytes of inference texts per batch. When set to a positive value, enables size-based batching. |

**Behavior:**
- If only `batch_size` is set: current behavior, unchanged.
- If only `batch_size_bytes` is set: batches are formed purely by byte size, with a floor of 1 document per batch (a single document that exceeds the byte limit is still sent as its own batch).
- If both are set: whichever limit is reached first triggers the batch to be dispatched. This is the recommended hybrid mode.
- The byte measurement is taken on the `inferenceList` (the actual text strings sent to ML Commons), not the entire document payload. This accurately reflects the size of the inference request.

**Index-level setting for semantic fields:**

A corresponding index-level setting will be added:

```
index.neural_search.semantic_ingest_batch_size_bytes
```

This mirrors the existing `index.neural_search.semantic_ingest_batch_size` setting for the `SemanticFieldProcessor`.

**No changes to existing REST APIs.** The `_ingest/pipeline` API already accepts arbitrary processor parameters. This is purely an additive configuration option.

#### Are there any security considerations?

No new security considerations. The feature does not introduce new APIs, endpoints, or authentication flows. It operates within the existing ingest pipeline security model. The byte threshold is a local configuration parameter that does not affect data access patterns.

One consideration: excessively large `batch_size_bytes` values could lead to high memory usage on ingest nodes. The implementation should enforce a reasonable upper bound and document recommended values.

#### Are there any breaking changes to the API?

No. This is a purely additive change. The new `batch_size_bytes` parameter is optional and defaults to `-1` (disabled), preserving existing behavior for all current users. Existing pipelines with only `batch_size` configured will continue to work identically.

### What is the user experience going to be?

1. **Default experience (no change):** Users who don't set `batch_size_bytes` see identical behavior to today. The static `batch_size` continues to control batching.

2. **Size-based batching:** Users set `batch_size_bytes` in their processor configuration. During bulk ingestion, the processor accumulates documents into a batch, measuring the cumulative byte size of the inference texts. When the byte threshold is reached, the batch is dispatched to ML Commons. This naturally produces smaller batches for large documents and larger batches for small documents.

3. **Hybrid batching (recommended):** Users set both `batch_size` and `batch_size_bytes`. The processor dispatches a batch when either limit is reached first. This provides dual safety bounds — preventing both excessively large payloads and excessively large document counts.

4. **Observability:** Batch dispatch metrics (batch count, average batch size in docs, average batch size in bytes) should be exposed through the existing neural search stats API (`_plugins/_neural_search/stats`) to help users tune their configuration.

**Example scenarios with `batch_size: 100, batch_size_bytes: 102400` (100KB):**

| Document profile | Effective batch size | Limiting factor |
|---|---|---|
| 100 docs × 500 bytes each (50KB total) | 100 docs | `batch_size` hit first |
| 5 docs × 25KB each (125KB total) | 4 docs | `batch_size_bytes` hit at ~100KB |
| 1 doc × 200KB | 1 doc | Floor of 1 doc (exceeds byte limit) |
| 1000 docs × 100 bytes each (100KB total) | 100 docs | `batch_size` hit first |

#### Are there breaking changes to the User Experience?

No. Existing behavior is preserved by default. Users opt in to size-based batching by explicitly configuring `batch_size_bytes`.

### Why should it be built? Any reason not to?

**Why build it:**
- The current static `batch_size` is a known pain point for users with heterogeneous document sizes. It forces a compromise: set it low enough for the largest documents (wasting throughput on small documents) or set it high for small documents (risking failures on large documents).
- Remote ML model providers increasingly enforce token/byte-based rate limits. Size-aware batching aligns the ingest processor with these real-world constraints, reducing throttling errors and improving ingestion reliability.
- This is a low-risk, high-value improvement. It's additive, backward-compatible, and addresses a fundamental limitation in the current batching model.
- The `SemanticFieldProcessorFactory` already has a [TODO to make batch size configurable](https://github.com/opensearch-project/neural-search/blob/main/src/main/java/org/opensearch/neuralsearch/processor/factory/SemanticFieldProcessorFactory.java#L43). Size-based batching would be a natural extension of that work.

**Reasons not to:**
- Adds configuration complexity. Users now have two knobs to tune instead of one. Mitigation: good defaults and documentation. The hybrid mode with both limits is straightforward to reason about.
- Byte measurement has a small runtime cost (measuring string byte lengths). This is negligible compared to the inference call itself.
- The batching split currently happens in OpenSearch core's `AbstractBatchingProcessor`. Implementing size-based splitting may require either a core change or a secondary split within the plugin layer. The plugin-layer approach is more practical for an initial implementation but may result in sub-batches within the core's document-count batches.

### What will it take to execute?

**Implementation approach:**

The most practical approach is to implement size-based splitting within the neural search plugin layer, inside `InferenceProcessor.doSubBatchExecute()`, rather than modifying OpenSearch core's `AbstractBatchingProcessor`. This avoids a core dependency and keeps the change scoped to the neural search plugin.

1. **Configuration parsing:** Update processor factories (`TextEmbeddingProcessorFactory`, `SparseEncodingProcessorFactory`, `SemanticFieldProcessorFactory`) to read `batch_size_bytes` from the processor config and pass it to the processor constructors.

2. **Byte-aware batch splitting:** In `InferenceProcessor`, after `constructInferenceTexts()` builds the flat inference list, split it into sub-batches based on cumulative byte size (using `String.getBytes(StandardCharsets.UTF_8).length` or a more efficient estimation). Each sub-batch is sent to `doBatchExecute()` independently.

3. **Result stitching:** Collect results from all sub-batches and concatenate them before passing to `batchExecuteHandler()` for mapping back to documents.

4. **Stats integration:** Add batch size metrics (doc count and byte size per batch) to the neural search stats framework.

**Assumptions and constraints:**
- Byte measurement is based on the UTF-8 encoded size of the inference text strings, not the full document payload.
- A single document that exceeds `batch_size_bytes` is always sent as its own batch (floor of 1).
- The existing `batch_size` from `AbstractBatchingProcessor` in OpenSearch core continues to control the initial document-level batching. The size-based splitting is a secondary split within each core-level batch. A future OpenSearch core enhancement could unify both into a single batching layer.
- No dependency on other feature work, though this complements the ongoing semantic field batch size configurability work ([neural-search#1438](https://github.com/opensearch-project/neural-search/pull/1438)).

**Estimated scope:** Medium. The core logic change is contained within `InferenceProcessor` and the processor factories. Testing requires benchmarking with varied document sizes to validate the adaptive behavior.

### Any remaining open questions?

1. **Should `batch_size_bytes` be measured on raw text or estimated tokens?** Byte size is simpler and model-agnostic, but token count would be more accurate for token-based rate limits. An initial implementation using bytes is recommended, with token estimation as a future enhancement.

2. **Should there be a cluster-level default for `batch_size_bytes`?** Similar to `index.neural_search.semantic_ingest_batch_size`, a cluster or index-level setting could provide a default that individual pipelines can override.

3. **Should the core `AbstractBatchingProcessor` be enhanced to support size-based batching natively?** This would be cleaner architecturally but requires an OpenSearch core change. The plugin-layer approach is recommended for the initial implementation, with a potential core RFC to follow.

4. **What is the right default value for `batch_size_bytes`?** This depends on typical ML Commons deployment configurations and remote model limits. Benchmarking across common model providers (SageMaker, Bedrock, OpenAI-compatible endpoints) would inform a sensible default. Candidates include 100KB, 256KB, or 512KB.

5. **Should the feature support per-field byte budgets for multi-field processors?** In the current design, the byte budget applies to the entire inference list across all fields in the `field_map`. Per-field budgets would add complexity but could be useful for processors with many mapped fields of varying sizes.
