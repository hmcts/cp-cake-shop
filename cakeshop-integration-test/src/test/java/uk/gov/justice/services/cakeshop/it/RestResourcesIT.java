package uk.gov.justice.services.cakeshop.it;

import static com.jayway.jsonpath.JsonPath.read;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.EVENT_DISCOVERY_URI;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.EVENT_RESOURCE_URI_TEMPLATE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAMS_QUERY_BASE_URI;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAMS_QUERY_BY_ERROR_HASH_URI_TEMPLATE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAMS_QUERY_BY_HAS_ERROR;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAMS_QUERY_BY_STREAM_ID_URI_TEMPLATE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAM_ERRORS_QUERY_BASE_URI;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAM_ERRORS_QUERY_BY_ERROR_ID_URI_TEMPLATE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAM_ERRORS_QUERY_BY_STREAM_ID_URI_TEMPLATE;

import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.LinkedEventInserter;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.cakeshop.it.helpers.TestDataManager;
import uk.gov.justice.services.common.converter.jackson.ObjectMapperProducer;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.messaging.DefaultJsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.ZonedDateTime;
import java.util.List;
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

import org.junit.jupiter.api.extension.ExtendWith;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseResetExtension;

@ExtendWith(DatabaseResetExtension.class)
public class RestResourcesIT {

    private final DataSource viewStoreDataSource = new DatabaseManager().initViewStoreDb();
    private final DataSource eventStoreDataSource = new DatabaseManager().initEventStoreDb();
    private final TestDataManager testDataManager = new TestDataManager(viewStoreDataSource);
    private final LinkedEventInserter linkedEventInserter = new LinkedEventInserter(eventStoreDataSource);
    private final ObjectMapper objectMapper = new ObjectMapperProducer().objectMapper();

    private DefaultJsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    private Client client;

    @BeforeEach
    public void before() throws Exception {
        client = new RestEasyClientFactory().createResteasyClient();
        jsonObjectEnvelopeConverter = new DefaultJsonObjectEnvelopeConverter();
        ReflectionUtil.setField(jsonObjectEnvelopeConverter, "objectMapper", objectMapper);

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
            final UUID errorId = streamError.get().streamErrorOccurrence().id();
            final UUID streamId = streamError.get().streamErrorOccurrence().streamId();
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

            await().until(() -> {
                final Invocation.Builder byErrorHashRequest = client.target(STREAMS_QUERY_BY_ERROR_HASH_URI_TEMPLATE.formatted(errorHash)).request();
                try (final Response response = byErrorHashRequest.get()) {
                    assertThat(response.getStatus(), is(200));
                    var actualResponse = response.readEntity(String.class);
                    assertEquals(expectedResponseWithErrorStreams, actualResponse, LENIENT);
                    return true;
                }
            });

            await().until(() -> {
                final Invocation.Builder byStreamIdRequest = client.target(STREAMS_QUERY_BY_STREAM_ID_URI_TEMPLATE.formatted(streamId)).request();
                try (final Response response = byStreamIdRequest.get()) {
                    assertThat(response.getStatus(), is(200));
                    var actualResponse = response.readEntity(String.class);
                    assertEquals(expectedResponseByStreamId, actualResponse, LENIENT);
                    return true;
                }
            });

            await().until(() -> {
                final Invocation.Builder getErroredStreamsRequest = client.target(STREAMS_QUERY_BY_HAS_ERROR).request();
                try (final Response response = getErroredStreamsRequest.get()) {
                    assertThat(response.getStatus(), is(200));
                    var actualResponse = response.readEntity(String.class);
                    assertEquals(expectedResponseWithErrorStreams, actualResponse, LENIENT);
                    return true;
                }
            });
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
            final UUID errorId = streamError.get().streamErrorOccurrence().id();
            final UUID streamId = streamError.get().streamErrorOccurrence().streamId();

            final Invocation.Builder byStreamIdRequest = client.target(STREAM_ERRORS_QUERY_BY_STREAM_ID_URI_TEMPLATE.formatted(streamId)).request();
            try (final Response response = byStreamIdRequest.get()) {
                assertThat(response.getStatus(), is(200));
                var actualResponse = response.readEntity(String.class);
                assertThat(read(actualResponse, "$[0].streamErrorOccurrence.causeMessage"), containsString("violates not-null constraint"));
                assertThat(read(actualResponse, "$[0].streamErrorOccurrence.eventId"), notNullValue());
                assertThat(read(actualResponse, "$[0].streamErrorOccurrence.fullStackTrace"), containsString("javax.persistence.PersistenceException:"));
            }

            final Invocation.Builder byErrorHashRequest = client.target(STREAM_ERRORS_QUERY_BY_ERROR_ID_URI_TEMPLATE.formatted(errorId)).request();
            try (final Response response = byErrorHashRequest.get()) {
                assertThat(response.getStatus(), is(200));
                var actualResponse = response.readEntity(String.class);
                assertThat(read(actualResponse, "$[0].streamErrorOccurrence.causeMessage"), containsString("violates not-null constraint"));
                assertThat(read(actualResponse, "$[0].streamErrorOccurrence.eventId"), notNullValue());
                assertThat(read(actualResponse, "$[0].streamErrorOccurrence.fullStackTrace"), containsString("javax.persistence.PersistenceException:"));
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

    @Nested
    class EventDiscoveryResourceIT {

        @Test
        public void shouldReturnEmptyDiscoveryResultWhenNoEventsPresent() throws JsonProcessingException {
            final Invocation.Builder request = client.target(EVENT_DISCOVERY_URI + "?batchSize=10").request();
            try (final Response response = request.get()) {
                assertThat(response.getStatus(), is(200));
                final DiscoveryResult discoveryResult = objectMapper.readValue(response.readEntity(String.class), DiscoveryResult.class);
                assertThat(discoveryResult.streamPositions(), is(emptyList()));
                assertThat(discoveryResult.latestKnownEventId(), is(Optional.empty()));
            }
        }

        @Test
        public void shouldDiscoverEventsByCriteria() throws JsonProcessingException {
            final UUID event1Id = randomUUID();
            final UUID stream1Id = randomUUID();
            final UUID event2Id = randomUUID();
            final UUID stream2Id = randomUUID();

            linkedEventInserter.insert(new LinkedEvent(
                    event1Id,
                    stream1Id,
                    1L,
                    "cakeshop.events.recipe-added",
                    "{\"id\":\"" + event1Id + "\",\"name\":\"cakeshop.events.recipe-added\",\"stream\":{\"id\":\"" + stream1Id + "\",\"version\":1},\"source\":\"cakeshop\",\"createdAt\":\"2024-01-01T00:00:00.0Z\"}",
                    "{\"recipeId\":\"" + stream1Id + "\"}",
                    ZonedDateTime.now(UTC),
                    1L,
                    0L
            ));
            linkedEventInserter.insert(new LinkedEvent(
                    event2Id,
                    stream2Id,
                    1L,
                    "cakeshop.events.recipe-added",
                    "{\"id\":\"" + event2Id + "\",\"name\":\"cakeshop.events.recipe-added\",\"stream\":{\"id\":\"" + stream2Id + "\",\"version\":1},\"source\":\"cakeshop\",\"createdAt\":\"2024-01-01T00:00:00.0Z\"}",
                    "{\"recipeId\":\"" + stream2Id + "\"}",
                    ZonedDateTime.now(UTC),
                    2L,
                    1L
            ));

            final Invocation.Builder allEventsRequest = client.target(EVENT_DISCOVERY_URI + "?batchSize=10").request();
            try (final Response response = allEventsRequest.get()) {
                assertThat(response.getStatus(), is(200));
                final DiscoveryResult discoveryResult = objectMapper.readValue(response.readEntity(String.class), DiscoveryResult.class);
                final List<UUID> streamIds = discoveryResult.streamPositions().stream()
                        .map(StreamPosition::streamId)
                        .toList();
                assertThat(streamIds, is(List.of(stream1Id, stream2Id)));
                assertThat(discoveryResult.latestKnownEventId(), is(of(event2Id)));
            }

            final Invocation.Builder afterEvent1Request = client.target(EVENT_DISCOVERY_URI + "?afterEventId=" + event1Id + "&batchSize=10").request();
            try (final Response response = afterEvent1Request.get()) {
                assertThat(response.getStatus(), is(200));
                final DiscoveryResult discoveryResult = objectMapper.readValue(response.readEntity(String.class), DiscoveryResult.class);
                final List<UUID> streamIds = discoveryResult.streamPositions().stream()
                        .map(StreamPosition::streamId)
                        .toList();
                assertThat(streamIds, is(singletonList(stream2Id)));
                assertThat(discoveryResult.latestKnownEventId(), is(of(event2Id)));
            }
        }
    }

    @Nested
    class EventResourceIT {

        @Test
        public void shouldReturnNextEventAfterPosition() throws JsonProcessingException {
            final UUID eventId = randomUUID();
            final UUID streamId = randomUUID();
            final String eventName = "cakeshop.events.recipe-added";
            final String metadata = """
                    {
                      "id": "%s",
                      "name": "%s",
                      "stream": {
                        "id": "%s",
                        "version": 1
                      },
                      "source": "cakeshop",
                      "createdAt": "2024-01-01T00:00:00.0Z"
                    }
                    """.formatted(eventId, eventName, streamId);
            final String payload = """
                    {
                        "recipeId" : "%s"
                    }
                    """.formatted(streamId);
            linkedEventInserter.insert(new LinkedEvent(
                    eventId,
                    streamId,
                    1L,
                    eventName,
                    metadata,
                    payload,
                    ZonedDateTime.now(UTC),
                    1L,
                    0L
            ));

            final Invocation.Builder request = client.target(EVENT_RESOURCE_URI_TEMPLATE.formatted(streamId) + "?afterPosition=0").request();
            try (final Response response = request.get()) {
                assertThat(response.getStatus(), is(200));

                JsonEnvelope jsonEnvelope = jsonObjectEnvelopeConverter.asEnvelope(response.readEntity(String.class));
                Metadata metadataFromResponse = jsonEnvelope.metadata();
                assertThat(metadataFromResponse.id(), is(eventId));
                assertThat(metadataFromResponse.name(), is(eventName));
                assertThat(metadataFromResponse.streamId().get(), is(streamId));
                assertThat(metadataFromResponse.source().get(), is("cakeshop"));
                assertThat(read(jsonEnvelope.payload().toString(), "$.recipeId"), is(streamId.toString()));
            }
        }

        @Test
        public void shouldReturn204WhenNoMoreEventsAfterPosition() {
            final UUID eventId = randomUUID();
            final UUID streamId = randomUUID();

            linkedEventInserter.insert(new LinkedEvent(
                    eventId,
                    streamId,
                    1L,
                    "cakeshop.events.recipe-added",
                    "{\"id\":\"" + eventId + "\",\"name\":\"cakeshop.events.recipe-added\",\"stream\":{\"id\":\"" + streamId + "\",\"version\":1},\"source\":\"cakeshop\",\"createdAt\":\"2024-01-01T00:00:00.0Z\"}",
                    "{\"recipeId\":\"" + streamId + "\"}",
                    ZonedDateTime.now(UTC),
                    1L,
                    0L
            ));

            final Invocation.Builder request = client.target(EVENT_RESOURCE_URI_TEMPLATE.formatted(streamId) + "?afterPosition=1").request();
            try (final Response response = request.get()) {
                assertThat(response.getStatus(), is(204));
            }
        }

        @Test
        public void shouldReturn204WhenStreamDoesNotExist() {
            final UUID nonExistentStreamId = randomUUID();
            final Invocation.Builder request = client.target(EVENT_RESOURCE_URI_TEMPLATE.formatted(nonExistentStreamId) + "?afterPosition=0").request();
            try (final Response response = request.get()) {
                assertThat(response.getStatus(), is(204));
            }
        }
    }
}

