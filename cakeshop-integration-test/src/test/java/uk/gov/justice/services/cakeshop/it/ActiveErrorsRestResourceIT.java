package uk.gov.justice.services.cakeshop.it;

import java.util.Optional;
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
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import static com.jayway.jsonassert.JsonAssert.with;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.ACTIVE_STREAM_ERRORS_QUERY_BASE_URI;

public class ActiveErrorsRestResourceIT {

    private final DataSource viewStoreDataSource = new DatabaseManager().initViewStoreDb();
    private final TestDataManager testDataManager = new TestDataManager(viewStoreDataSource);
    final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

    private Client client;

    @BeforeEach
    public void before() throws Exception {
        client = new RestEasyClientFactory().createResteasyClient();

        databaseCleaner.cleanEventStoreTables("cakeshop");
        databaseCleaner.cleanViewStoreTables(
                "cakeshop",
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
    class ActiveStreamErrorsResourceIT {

        @Test
        public void shouldGetTheJsonForActiveErrorsInTheStreamErrorTables() throws Exception {
            final Optional<StreamError> streamErrorOptional = testDataManager.createAnEventWithEventListenerFailure();
            if (streamErrorOptional.isEmpty()) {
                fail();
            }

            final Invocation.Builder request = client.target(ACTIVE_STREAM_ERRORS_QUERY_BASE_URI).request();
            try (final Response response = request.get()) {
                assertThat(response.getStatus(), is(200));

                final String responseJson = response.readEntity(String.class);

                final StreamError streamError = streamErrorOptional.get();
                final StreamErrorHash streamErrorHash = streamError.streamErrorHash();
                assertThat(streamErrorHash.causeClassName().isPresent(), is(true));

                with(responseJson)
                        .assertThat("$[0].hash", is(streamErrorHash.hash()))
                        .assertThat("$[0].exceptionClassname", is(streamErrorHash.exceptionClassName()))
                        .assertThat("$[0].causeClassname", is(streamErrorHash.causeClassName().get()))
                        .assertThat("$[0].javaClassname", is(streamErrorHash.javaClassName()))
                        .assertThat("$[0].javaMethod", is(streamErrorHash.javaMethod()))
                        .assertThat("$[0].affectedStreamsCount", is(1))
                        .assertThat("$[0].affectedEventsCount", is(0))
                ;
            }
        }
    }
}

