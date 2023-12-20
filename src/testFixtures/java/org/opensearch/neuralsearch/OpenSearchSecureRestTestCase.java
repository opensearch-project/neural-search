/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch;

import static org.opensearch.client.RestClientBuilder.DEFAULT_MAX_CONN_PER_ROUTE;
import static org.opensearch.client.RestClientBuilder.DEFAULT_MAX_CONN_TOTAL;
import static org.opensearch.knn.common.KNNConstants.MODELS;
import static org.opensearch.knn.common.KNNConstants.MODEL_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.NEURAL_SEARCH_BWC_PREFIX;
import static org.opensearch.neuralsearch.TestUtils.OPENDISTRO_SECURITY;
import static org.opensearch.neuralsearch.TestUtils.OPENSEARCH_SYSTEM_INDEX_PREFIX;
import static org.opensearch.neuralsearch.TestUtils.SECURITY_AUDITLOG_PREFIX;
import static org.opensearch.neuralsearch.TestUtils.SKIP_DELETE_MODEL_INDEX;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.junit.After;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.*;
import org.opensearch.knn.plugin.KNNPlugin;
import org.opensearch.search.SearchHit;
import org.opensearch.test.rest.OpenSearchRestTestCase;

/**
 * Base class for running the integration tests on a secure cluster. The plugin IT test should either extend this
 * class or create another base class by extending this class to make sure that their IT can be run on secure clusters.
 */
public abstract class OpenSearchSecureRestTestCase extends OpenSearchRestTestCase {

    private static final String PROTOCOL_HTTP = "http";
    private static final String PROTOCOL_HTTPS = "https";
    private static final String SYS_PROPERTY_KEY_HTTPS = "https";
    private static final String SYS_PROPERTY_KEY_CLUSTER_ENDPOINT = "tests.rest.cluster";
    private static final String SYS_PROPERTY_KEY_USER = "user";
    private static final String SYS_PROPERTY_KEY_PASSWORD = "password";
    private static final String DEFAULT_SOCKET_TIMEOUT = "60s";
    private static final String INTERNAL_INDICES_PREFIX = ".";
    private static String protocol;

    private final Set<String> IMMUTABLE_INDEX_PREFIXES = Set.of(
        NEURAL_SEARCH_BWC_PREFIX,
        SECURITY_AUDITLOG_PREFIX,
        OPENSEARCH_SYSTEM_INDEX_PREFIX
    );

    @Override
    protected String getProtocol() {
        if (protocol == null) {
            protocol = readProtocolFromSystemProperty();
        }
        return protocol;
    }

    private String readProtocolFromSystemProperty() {
        final boolean isHttps = Optional.ofNullable(System.getProperty(SYS_PROPERTY_KEY_HTTPS)).map("true"::equalsIgnoreCase).orElse(false);
        if (!isHttps) {
            return PROTOCOL_HTTP;
        }

        // currently only external cluster is supported for security enabled testing
        if (Optional.ofNullable(System.getProperty(SYS_PROPERTY_KEY_CLUSTER_ENDPOINT)).isEmpty()) {
            throw new RuntimeException("cluster url should be provided for security enabled testing");
        }
        return PROTOCOL_HTTPS;
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        final RestClientBuilder builder = RestClient.builder(hosts);
        if (PROTOCOL_HTTPS.equals(getProtocol())) {
            configureHttpsClient(builder, settings);
        } else {
            configureClient(builder, settings);
        }

        return builder.build();
    }

    private void configureHttpsClient(final RestClientBuilder builder, final Settings settings) {
        final Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
        final Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            final String userName = Optional.ofNullable(System.getProperty(SYS_PROPERTY_KEY_USER))
                .orElseThrow(() -> new RuntimeException("user name is missing"));
            final String password = Optional.ofNullable(System.getProperty(SYS_PROPERTY_KEY_PASSWORD))
                .orElseThrow(() -> new RuntimeException("password is missing"));
            final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            final AuthScope anyScope = new AuthScope(null, -1);
            credentialsProvider.setCredentials(anyScope, new UsernamePasswordCredentials(userName, password.toCharArray()));
            try {
                final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                    .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setSslContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build())
                    .build();
                final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                    .setMaxConnPerRoute(DEFAULT_MAX_CONN_PER_ROUTE)
                    .setMaxConnTotal(DEFAULT_MAX_CONN_TOTAL)
                    .setTlsStrategy(tlsStrategy)
                    .build();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout = TimeValue.parseTimeValue(
            socketTimeoutString == null ? DEFAULT_SOCKET_TIMEOUT : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT
        );
        builder.setRequestConfigCallback(conf -> {
            Timeout timeout = Timeout.ofMilliseconds(Math.toIntExact(socketTimeout.getMillis()));
            conf.setConnectTimeout(timeout);
            conf.setResponseTimeout(timeout);
            return conf;
        });
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    /**
     * wipeAllIndices won't work since it cannot delete security index. Use deleteExternalIndices instead.
     */
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @After
    public void deleteExternalIndices() throws IOException, ParseException {
        final Response response = client().performRequest(new Request("GET", "/_cat/indices?format=json" + "&expand_wildcards=all"));
        final MediaType xContentType = MediaType.fromMediaType(response.getEntity().getContentType());
        try (
            final XContentParser parser = xContentType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
        ) {
            final XContentParser.Token token = parser.nextToken();
            final List<Map<String, Object>> parserList;
            if (token == XContentParser.Token.START_ARRAY) {
                parserList = parser.listOrderedMap().stream().map(obj -> (Map<String, Object>) obj).collect(Collectors.toList());
            } else {
                parserList = Collections.singletonList(parser.mapOrdered());
            }

            final List<String> externalIndices = parserList.stream()
                .map(index -> (String) index.get("index"))
                .filter(indexName -> indexName != null)
                .filter(indexName -> !indexName.startsWith(INTERNAL_INDICES_PREFIX))
                .collect(Collectors.toList());

            for (final String indexName : externalIndices) {
                if (isIndexCleanupRequired(indexName)) {
                    wipeIndexContent(indexName);
                    continue;
                }
                if (!skipDeleteIndex(indexName)) {
                    adminClient().performRequest(new Request("DELETE", "/" + indexName));
                }
            }
        }
    }

    private boolean isIndexCleanupRequired(final String index) {
        return MODEL_INDEX_NAME.equals(index) && !getSkipDeleteModelIndexFlag();
    }

    private void wipeIndexContent(String indexName) throws IOException, ParseException {
        deleteModels(getModelIds());
        deleteAllDocs(indexName);
    }

    private List<String> getModelIds() throws IOException, ParseException {
        final String restURIGetModels = String.join("/", KNNPlugin.KNN_BASE_URI, MODELS, "_search");
        final Response response = adminClient().performRequest(new Request("GET", restURIGetModels));

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        final String responseBody = EntityUtils.toString(response.getEntity());
        assertNotNull(responseBody);

        final XContentParser parser = createParser(MediaTypeRegistry.getDefaultMediaType().xContent(), responseBody);
        final SearchResponse searchResponse = SearchResponse.fromXContent(parser);

        return Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toList());
    }

    private void deleteModels(final List<String> modelIds) throws IOException {
        for (final String testModelID : modelIds) {
            final String restURIGetModel = String.join("/", KNNPlugin.KNN_BASE_URI, MODELS, testModelID);
            final Response getModelResponse = adminClient().performRequest(new Request("GET", restURIGetModel));
            if (RestStatus.OK != RestStatus.fromCode(getModelResponse.getStatusLine().getStatusCode())) {
                continue;
            }
            final String restURIDeleteModel = String.join("/", KNNPlugin.KNN_BASE_URI, MODELS, testModelID);
            adminClient().performRequest(new Request("DELETE", restURIDeleteModel));
        }
    }

    private void deleteAllDocs(final String indexName) throws IOException {
        final String restURIDeleteByQuery = String.join("/", indexName, "_delete_by_query");
        final Request request = new Request("POST", restURIDeleteByQuery);
        final XContentBuilder matchAllDocsQuery = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("query")
            .startObject("match_all")
            .endObject()
            .endObject()
            .endObject();

        request.setJsonEntity(matchAllDocsQuery.toString());
        adminClient().performRequest(request);
    }

    private boolean getSkipDeleteModelIndexFlag() {
        return Boolean.parseBoolean(System.getProperty(SKIP_DELETE_MODEL_INDEX, "false"));
    }

    private boolean skipDeleteModelIndex(String indexName) {
        return (MODEL_INDEX_NAME.equals(indexName) && getSkipDeleteModelIndexFlag());
    }

    private boolean skipDeleteIndex(String indexName) {
        if (indexName != null
            && !OPENDISTRO_SECURITY.equals(indexName)
            && IMMUTABLE_INDEX_PREFIXES.stream().noneMatch(indexName::startsWith)
            && !skipDeleteModelIndex(indexName)) {
            return false;
        }

        return true;
    }
}
