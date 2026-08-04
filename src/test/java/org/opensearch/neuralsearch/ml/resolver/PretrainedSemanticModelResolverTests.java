/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml.resolver;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.MLModelState;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PretrainedSemanticModelResolverTests extends OpenSearchTestCase {

    @Mock
    private MachineLearningNodeClient mlClient;

    @Mock
    private Client client;

    @Mock
    private ThreadPool threadPool;

    private PretrainedSemanticModelResolver resolver;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        ThreadContext threadContext = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        resolver = new PretrainedSemanticModelResolver(mlClient, client);
    }

    public void testValidate_rejectsUnknownLanguageOption() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> resolver.validate("KLINGON", "SPARSE"));
        assertTrue(exception.getMessage().contains("Unknown language_option [KLINGON]"));
    }

    public void testValidate_rejectsUnknownModelType() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> resolver.validate("ENGLISH", "TRANSFORMER")
        );
        assertTrue(exception.getMessage().contains("Unknown model_type [TRANSFORMER]"));
    }

    public void testValidate_acceptsValidCombinations() {
        // Should not throw
        resolver.validate("ENGLISH", "SPARSE");
        resolver.validate("ENGLISH", "DENSE");
        resolver.validate("MULTILINGUAL", "SPARSE");
        resolver.validate("MULTILINGUAL", "DENSE");
    }

    public void testValidate_acceptsNullValues() {
        // Null values should pass validation (they get defaults later)
        resolver.validate(null, null);
        resolver.validate(null, "SPARSE");
        resolver.validate("ENGLISH", null);
    }

    public void testResolve_returnsCachedValueOnSecondCall() {
        // Set up mock to return model found in search on first call
        SearchHit hit = new SearchHit(1, "cached_model_id", null, null);
        SearchHits searchHits = new SearchHits(new SearchHit[] { hit }, null, 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        SearchResponse searchResponse = new SearchResponse(sections, null, 1, 1, 0, 100, new ShardSearchFailure[] {}, null);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        MLModel mockModel = mock(MLModel.class);
        when(mockModel.getModelState()).thenReturn(MLModelState.DEPLOYED);

        doAnswer(invocation -> {
            ActionListener<MLModel> listener = invocation.getArgument(2);
            listener.onResponse(mockModel);
            return null;
        }).when(mlClient).getModel(eq("cached_model_id"), isNull(), any(ActionListener.class));

        // First call
        AtomicReference<String> result1 = new AtomicReference<>();
        resolver.resolve("ENGLISH", "SPARSE", ActionListener.wrap(result1::set, e -> fail("Should not fail")));
        assertEquals("cached_model_id", result1.get());

        // Second call - should use cache, not call client again
        AtomicReference<String> result2 = new AtomicReference<>();
        resolver.resolve("ENGLISH", "SPARSE", ActionListener.wrap(result2::set, e -> fail("Should not fail")));
        assertEquals("cached_model_id", result2.get());

        // Verify search was only called once (cached for second call)
        verify(client, times(1)).search(any(SearchRequest.class), any(ActionListener.class));
    }

    public void testResolve_englishSparseModel() {
        setupSearchNotFound();
        setupRegisterSuccess("english_sparse_id");

        AtomicReference<String> result = new AtomicReference<>();
        resolver.resolve("ENGLISH", "SPARSE", ActionListener.wrap(result::set, e -> fail("Should not fail: " + e.getMessage())));

        assertEquals("english_sparse_id", result.get());

        // Verify the model name is correct
        ArgumentCaptor<MLRegisterModelInput> captor = ArgumentCaptor.forClass(MLRegisterModelInput.class);
        verify(mlClient).register(captor.capture(), any(ActionListener.class));
        assertEquals("amazon/neural-sparse/opensearch-neural-sparse-encoding-doc-v2-mini", captor.getValue().getModelName());
        assertEquals("1.0.0", captor.getValue().getVersion());
        assertEquals(FunctionName.SPARSE_ENCODING, captor.getValue().getFunctionName());
    }

    public void testResolve_englishDenseModel() {
        setupSearchNotFound();
        setupRegisterSuccess("english_dense_id");

        AtomicReference<String> result = new AtomicReference<>();
        resolver.resolve("ENGLISH", "DENSE", ActionListener.wrap(result::set, e -> fail("Should not fail: " + e.getMessage())));

        assertEquals("english_dense_id", result.get());

        ArgumentCaptor<MLRegisterModelInput> captor = ArgumentCaptor.forClass(MLRegisterModelInput.class);
        verify(mlClient).register(captor.capture(), any(ActionListener.class));
        assertEquals("huggingface/sentence-transformers/paraphrase-MiniLM-L3-v2", captor.getValue().getModelName());
        assertEquals("1.0.1", captor.getValue().getVersion());
        assertEquals(FunctionName.TEXT_EMBEDDING, captor.getValue().getFunctionName());
    }

    public void testResolve_multilingualSparseModel() {
        setupSearchNotFound();
        setupRegisterSuccess("multilingual_sparse_id");

        AtomicReference<String> result = new AtomicReference<>();
        resolver.resolve("MULTILINGUAL", "SPARSE", ActionListener.wrap(result::set, e -> fail("Should not fail: " + e.getMessage())));

        assertEquals("multilingual_sparse_id", result.get());

        ArgumentCaptor<MLRegisterModelInput> captor = ArgumentCaptor.forClass(MLRegisterModelInput.class);
        verify(mlClient).register(captor.capture(), any(ActionListener.class));
        assertEquals("amazon/neural-sparse/opensearch-neural-sparse-encoding-multilingual-v1", captor.getValue().getModelName());
        assertEquals("1.0.1", captor.getValue().getVersion());
        assertEquals(FunctionName.SPARSE_ENCODING, captor.getValue().getFunctionName());
    }

    public void testResolve_multilingualDenseModel() {
        setupSearchNotFound();
        setupRegisterSuccess("multilingual_dense_id");

        AtomicReference<String> result = new AtomicReference<>();
        resolver.resolve("MULTILINGUAL", "DENSE", ActionListener.wrap(result::set, e -> fail("Should not fail: " + e.getMessage())));

        assertEquals("multilingual_dense_id", result.get());

        ArgumentCaptor<MLRegisterModelInput> captor = ArgumentCaptor.forClass(MLRegisterModelInput.class);
        verify(mlClient).register(captor.capture(), any(ActionListener.class));
        assertEquals("huggingface/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", captor.getValue().getModelName());
        assertEquals("1.0.2", captor.getValue().getVersion());
        assertEquals(FunctionName.TEXT_EMBEDDING, captor.getValue().getFunctionName());
    }

    public void testResolve_indexNotFoundFallsToRegister() {
        // Simulate IndexNotFoundException during search
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new IndexNotFoundException(".plugins-ml-model"));
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        setupRegisterSuccess("new_model_id");

        AtomicReference<String> result = new AtomicReference<>();
        resolver.resolve("ENGLISH", "SPARSE", ActionListener.wrap(result::set, e -> fail("Should not fail: " + e.getMessage())));

        assertEquals("new_model_id", result.get());
        verify(mlClient).register(any(MLRegisterModelInput.class), any(ActionListener.class));
    }

    public void testResolve_defaultsToEnglishSparse() {
        setupSearchNotFound();
        setupRegisterSuccess("default_model_id");

        AtomicReference<String> result = new AtomicReference<>();
        resolver.resolve(null, null, ActionListener.wrap(result::set, e -> fail("Should not fail: " + e.getMessage())));

        assertEquals("default_model_id", result.get());

        ArgumentCaptor<MLRegisterModelInput> captor = ArgumentCaptor.forClass(MLRegisterModelInput.class);
        verify(mlClient).register(captor.capture(), any(ActionListener.class));
        assertEquals("amazon/neural-sparse/opensearch-neural-sparse-encoding-doc-v2-mini", captor.getValue().getModelName());
    }

    private void setupSearchNotFound() {
        SearchHits emptyHits = new SearchHits(new SearchHit[] {}, null, 0.0f);
        SearchResponseSections sections = new SearchResponseSections(emptyHits, null, null, false, false, null, 0);
        SearchResponse emptyResponse = new SearchResponse(sections, null, 1, 1, 0, 100, new ShardSearchFailure[] {}, null);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(emptyResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    private void setupRegisterSuccess(String modelId) {
        MLRegisterModelResponse registerResponse = mock(MLRegisterModelResponse.class);
        when(registerResponse.getModelId()).thenReturn(modelId);
        when(registerResponse.getTaskId()).thenReturn(null);

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> listener = invocation.getArgument(1);
            listener.onResponse(registerResponse);
            return null;
        }).when(mlClient).register(any(MLRegisterModelInput.class), any(ActionListener.class));
    }
}
