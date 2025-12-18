package uk.gov.justice.services.cakeshop.it;

import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.cakeshop.it.helpers.TestDataManager;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import static com.jayway.jsonpath.JsonPath.read;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAMS_QUERY_BASE_URI;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAMS_QUERY_BY_ERROR_HASH_URI_TEMPLATE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAMS_QUERY_BY_HAS_ERROR;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAMS_QUERY_BY_STREAM_ID_URI_TEMPLATE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAM_ERRORS_QUERY_BASE_URI;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAM_ERRORS_QUERY_BY_ERROR_ID_URI_TEMPLATE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAM_ERRORS_QUERY_BY_STREAM_ID_URI_TEMPLATE;

public class RestResourcesIT {

    private final DataSource viewStoreDataSource = new DatabaseManager().initViewStoreDb();
    private final TestDataManager testDataManager = new TestDataManager(viewStoreDataSource);
    final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

    private Client client;

    @BeforeEach
    public void before() throws Exception {
        client = new RestEasyClientFactory().createResteasyClient();

        databaseCleaner.cleanEventStoreTables(CONTEXT_NAME);
        databaseCleaner.cleanViewStoreTables(
                CONTEXT_NAME,
                "stream_buffer",
                "stream_status",
                "stream_error_hash",
                "stream_error",
                "cake",
                "cake_order",
                "recipe",
                "ingredient",
                "processed_event");
    }

    @AfterEach
    public void cleanup() throws Exception {
        client.close();
    }


    @Nested
    class StreamsResourceIT {

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        @Test
        public void getStreamsByCriteria() {
            final Optional<StreamError> streamError = testDataManager.createAnEventWithEventListenerFailure();
            final String errorHash = streamError.get().streamErrorHash().hash();
            final UUID errorId = streamError.get().streamErrorDetails().id();
            final UUID streamId = streamError.get().streamErrorDetails().streamId();
            final String expectedResponseWithErrorStreams = """
                    [
                        {
                            "streamId": %s,
                            "position": 0,
                            "lastKnownPosition": 1,
                            "source": "cakeshop",
                            "component": "EVENT_LISTENER",
                            "upToDate": false,
                            "errorId": %s,
                            "errorPosition": 1
                        }
                    ]
                    """.formatted(streamId, errorId);

            final String expectedResponseByStreamId = """
                    [
                        {
                            "streamId": %s,
                            "position": 0,
                            "lastKnownPosition": 1,
                            "source": "cakeshop",
                            "component": "EVENT_LISTENER",
                            "upToDate": false,
                            "errorId": %s,
                            "errorPosition": 1
                        },
                        {
                            "streamId": %s,
                            "position": 1,
                            "lastKnownPosition": 1,
                            "source": "cakeshop",
                            "component": "EVENT_INDEXER",
                            "upToDate": true,
                            "errorId": null,
                            "errorPosition": null
                        }
                    ]
                    """.formatted(streamId, errorId, streamId);

            final Invocation.Builder byErrorHashRequest = client.target(STREAMS_QUERY_BY_ERROR_HASH_URI_TEMPLATE.formatted(errorHash)).request();
            try (final Response response = byErrorHashRequest.get()) {
                assertThat(response.getStatus(), is(200));
                var actualResponse = response.readEntity(String.class);
                assertEquals(expectedResponseWithErrorStreams, actualResponse, LENIENT);
            }

            final Invocation.Builder byStreamIdRequest = client.target(STREAMS_QUERY_BY_STREAM_ID_URI_TEMPLATE.formatted(streamId)).request();
            try (final Response response = byStreamIdRequest.get()) {
                assertThat(response.getStatus(), is(200));
                var actualResponse = response.readEntity(String.class);
                assertEquals(expectedResponseByStreamId, actualResponse, LENIENT);
            }

            final Invocation.Builder getErroredStreamsRequest = client.target(STREAMS_QUERY_BY_HAS_ERROR).request();
            try (final Response response = getErroredStreamsRequest.get()) {
                assertThat(response.getStatus(), is(200));
                var actualResponse = response.readEntity(String.class);
                assertEquals(expectedResponseWithErrorStreams, actualResponse, LENIENT);
            }
        }

        @Test
        public void shouldReturnBadRequestGivenInvalidInputs() {
            final Invocation.Builder requestWithNoQueryParams = client.target(STREAMS_QUERY_BASE_URI).request();
            try (final Response response = requestWithNoQueryParams.get()) {
                assertThat(response.getStatus(), is(400));
                var actualResponse = response.readEntity(String.class);
                String errorMessage = read(actualResponse, "$.errorMessage");
                assertThat(errorMessage, containsString("Exactly one query parameter(errorHash/streamId/hasError) must be provided"));
            }

            final Invocation.Builder requestWithNoInvalidValueForHasError = client.target(STREAMS_QUERY_BASE_URI + "?hasError=false").request();
            try (final Response response = requestWithNoInvalidValueForHasError.get()) {
                assertThat(response.getStatus(), is(400));
                var actualResponse = response.readEntity(String.class);
                String errorMessage = read(actualResponse, "$.errorMessage");
                assertThat(errorMessage, containsString("Accepted values for errorHash: true"));
            }
        }
    }

    @Nested
    class StreamErrorsResourceIT {

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        @Test
        public void getStreamErrorsByCriteria() {
            final Optional<StreamError> streamError = testDataManager.createAnEventWithEventListenerFailure();
            final UUID errorId = streamError.get().streamErrorDetails().id();
            final UUID streamId = streamError.get().streamErrorDetails().streamId();

            final Invocation.Builder byStreamIdRequest = client.target(STREAM_ERRORS_QUERY_BY_STREAM_ID_URI_TEMPLATE.formatted(streamId)).request();
            try (final Response response = byStreamIdRequest.get()) {
                assertThat(response.getStatus(), is(200));
                var actualResponse = response.readEntity(String.class);
                assertThat(read(actualResponse, "$[0].streamErrorDetails.causeMessage"), containsString("violates not-null constraint"));
                assertThat(read(actualResponse, "$[0].streamErrorDetails.eventId"), notNullValue());
                assertThat(read(actualResponse, "$[0].streamErrorDetails.fullStackTrace"), containsString("javax.persistence.PersistenceException:"));
            }

            final Invocation.Builder byErrorHashRequest = client.target(STREAM_ERRORS_QUERY_BY_ERROR_ID_URI_TEMPLATE.formatted(errorId)).request();
            try (final Response response = byErrorHashRequest.get()) {
                assertThat(response.getStatus(), is(200));
                var actualResponse = response.readEntity(String.class);
                assertThat(read(actualResponse, "$[0].streamErrorDetails.causeMessage"), containsString("violates not-null constraint"));
                assertThat(read(actualResponse, "$[0].streamErrorDetails.eventId"), notNullValue());
                assertThat(read(actualResponse, "$[0].streamErrorDetails.fullStackTrace"), containsString("javax.persistence.PersistenceException:"));
            }
        }

        @Test
        public void shouldReturnBadRequestGivenInvalidInputs() {
            final Invocation.Builder requestWithNoQueryParams = client.target(STREAM_ERRORS_QUERY_BASE_URI).request();
            try (final Response response = requestWithNoQueryParams.get()) {
                assertThat(response.getStatus(), is(400));
                var actualResponse = response.readEntity(String.class);
                String errorMessage = read(actualResponse, "$.errorMessage");
                assertThat(errorMessage, containsString("Please set either 'streamId' or 'errorId' as request parameters"));
            }

            final Invocation.Builder requestWithBothQueryParams = client.target(STREAM_ERRORS_QUERY_BASE_URI + "?streamId=%s&errorId=%s".formatted(randomUUID(), randomUUID())).request();
            try (final Response response = requestWithBothQueryParams.get()) {
                assertThat(response.getStatus(), is(400));
                var actualResponse = response.readEntity(String.class);
                String errorMessage = read(actualResponse, "$.errorMessage");
                assertThat(errorMessage, containsString("Please set either 'streamId' or 'errorId' as request parameters, not both"));
            }
        }
    }
}

