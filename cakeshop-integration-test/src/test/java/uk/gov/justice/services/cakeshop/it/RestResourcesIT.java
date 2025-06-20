package uk.gov.justice.services.cakeshop.it;

import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.cakeshop.it.helpers.TestDataManager;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.STREAMS_QUERY_BY_ERROR_HASH_URI_TEMPLATE;

public class RestResourcesIT {

    private final DataSource viewStoreDataSource = new DatabaseManager().initViewStoreDb();
    private final TestDataManager testDataManager = new TestDataManager(viewStoreDataSource);
    final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

    private Client client;

    @BeforeEach
    public void before() throws Exception {
        client = new RestEasyClientFactory().createResteasyClient();

        databaseCleaner.cleanEventStoreTables("framework");
        databaseCleaner.cleanViewStoreTables(
                "framework",
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


    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void getAllStreamsByErrorHash() {
        final Optional<StreamError> streamError = testDataManager.createAnEventWithViewStoreFailure();
        final String errorHash = streamError.get().streamErrorHash().hash();
        final UUID errorId = streamError.get().streamErrorDetails().id();
        final UUID streamId = streamError.get().streamErrorDetails().streamId();

        final Invocation.Builder getStreamsRequest = client.target(STREAMS_QUERY_BY_ERROR_HASH_URI_TEMPLATE.formatted(errorHash)).request();
        try (final Response response = getStreamsRequest.get()) {
            assertThat(response.getStatus(), is(200));
            final String expectedResponse = """
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
            var actualResponse = response.readEntity(String.class);
            assertEquals(expectedResponse, actualResponse, LENIENT);
        }
    }
}

