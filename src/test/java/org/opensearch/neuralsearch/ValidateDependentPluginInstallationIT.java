/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.rest.RestRequest;

/**
 * This test validates that dependent plugin defined in build.gradle -> opensearchplugin.extendedPlugins are
 * installed or not. We don't want to run these tests before every IT as it can slow down the execution of IT. Hence
 * doing it separately.
 */
public class ValidateDependentPluginInstallationIT extends OpenSearchSecureRestTestCase {

    private static final String KNN_INDEX_NAME = "neuralsearchknnindexforvalidation";
    private static final String KNN_VECTOR_FIELD_NAME = "vectorField";
    private static final Set<String> DEPENDENT_PLUGINS = Set.of("opensearch-ml", "opensearch-knn");
    private static final String GET_PLUGINS_URL = "_cat/plugins";
    private static final String ML_PLUGIN_STATS_URL = "_plugins/_ml/stats";
    private static final String KNN_DOCUMENT_URL = KNN_INDEX_NAME + "/_doc/1?refresh";

    public void testDependentPluginsInstalled() throws IOException {
        final Set<String> installedPlugins = getAllInstalledPlugins();

        // detect whether JVector or Knn plugin is loaded, and update dependent plugin list accordingly
        String jarPath = VectorDataType.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String jarFileName = jarPath.substring(jarPath.lastIndexOf('/') + 1);
        if (jarFileName.contains("jvector")) {
            DEPENDENT_PLUGINS.remove("opensearch-knn");
            DEPENDENT_PLUGINS.add("opensearch-jvector");
        }

        Assert.assertTrue(installedPlugins.containsAll(DEPENDENT_PLUGINS));
    }

    /**
     * Validate K-NN Plugin Setup by creating a k-NN index and then deleting the index. Not adding the index deletion
     * in the cleanup setup as index creation was not part of whole setup for this test cases class.
     */
    public void testValidateKNNPluginSetup() throws IOException {
        createBasicKnnIndex();
        Assert.assertTrue(indexExists(KNN_INDEX_NAME));
        indexDocument();
        getDocument();
        deleteIndex(KNN_INDEX_NAME);
        Assert.assertFalse(indexExists(KNN_INDEX_NAME));
    }

    /**
     * Validate ML-Plugin setup. We are using the stats API of ML Plugin to validate plugin is present or not. This
     * was the best API that we can use without creating any side effects.
     */
    public void testValidateMLPluginSetup() throws IOException {
        final Request request = new Request(RestRequest.Method.GET.name(), ML_PLUGIN_STATS_URL);
        assertOK(client().performRequest(request));
    }

    private void createBasicKnnIndex() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(KNN_VECTOR_FIELD_NAME)
            .field("type", "knn_vector")
            .field("dimension", Integer.toString(3))
            .startObject("method")
            .field("engine", "lucene")
            .field("name", "hnsw")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        mapping = mapping.substring(1, mapping.length() - 1);
        Settings settings = Settings.builder().put("index.knn", true).build();
        createIndex(KNN_INDEX_NAME, settings, mapping);
    }

    private Set<String> getAllInstalledPlugins() throws IOException {
        final Request request = new Request(RestRequest.Method.GET.name(), GET_PLUGINS_URL);
        final Response response = client().performRequest(request);
        assertOK(response);
        final BufferedReader responseReader = new BufferedReader(
            new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)
        );
        final Set<String> installedPluginsSet = new HashSet<>();
        String line;
        while ((line = responseReader.readLine()) != null) {
            // Output looks like this: integTest-0 opensearch-knn 2.3.0.0
            final String pluginName = line.split("\\s+")[1];
            installedPluginsSet.add(pluginName);
        }
        return installedPluginsSet;
    }

    private void indexDocument() throws IOException {
        final String indexRequestBody = XContentFactory.jsonBuilder()
            .startObject()
            .startArray(KNN_VECTOR_FIELD_NAME)
            .value(1.0)
            .value(2.0)
            .value(4.0)
            .endArray()
            .endObject()
            .toString();
        final Request indexRequest = new Request(RestRequest.Method.POST.name(), KNN_DOCUMENT_URL);
        indexRequest.setJsonEntity(indexRequestBody);
        assertOK(client().performRequest(indexRequest));
    }

    private void getDocument() throws IOException {
        final Request getDocumentRequest = new Request(RestRequest.Method.GET.name(), KNN_DOCUMENT_URL);
        assertOK(client().performRequest(getDocumentRequest));
    }
}
