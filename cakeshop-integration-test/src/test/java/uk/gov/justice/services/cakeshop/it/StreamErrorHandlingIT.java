package uk.gov.justice.services.cakeshop.it;

import java.util.Optional;
import javax.sql.DataSource;
import javax.ws.rs.client.Client;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.cakeshop.it.helpers.StreamStatusFinder;
import uk.gov.justice.services.cakeshop.it.helpers.TestDataManager;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.test.utils.core.messaging.Poller;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class StreamErrorHandlingIT {

    private final DataSource viewStoreDataSource = new DatabaseManager().initViewStoreDb();
    private final TestDataManager testDataManager = new TestDataManager(viewStoreDataSource);
    private final StreamStatusFinder streamStatusFinder = new StreamStatusFinder(viewStoreDataSource);
    final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

    private final Poller poller = new Poller(20, 1000L);

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
    public void shouldAddRowInStreamErrorTableOnEventProcessingFailure() throws Exception {
        final String eventName = "cakeshop.events.recipe-added";

        final Optional<StreamError> streamErrorOptional = testDataManager.createAnEventWithEventListenerFailure(eventName);

        if (streamErrorOptional.isPresent()) {
            final StreamError streamError = streamErrorOptional.get();
            final StreamErrorDetails streamErrorDetails = streamError.streamErrorDetails();
            final StreamErrorHash streamErrorHash = streamError.streamErrorHash();

            assertThat(streamErrorHash.exceptionClassName(), is("javax.persistence.PersistenceException"));
            assertThat(streamErrorHash.causeClassName(), is(of("org.postgresql.util.PSQLException")));
            assertThat(streamErrorHash.javaClassName(), is("uk.gov.justice.services.persistence.EntityManagerFlushInterceptor"));

            assertThat(streamErrorDetails.exceptionMessage(), is("org.hibernate.exception.ConstraintViolationException: could not execute statement"));
            assertThat(streamErrorDetails.causeMessage().get(), startsWith("ERROR: null value in column"));
            assertThat(streamErrorDetails.causeMessage().get(), containsString("violates not-null constraint"));

            final Optional<StreamStatusFinder.StreamStatus> streamStatus = poller.pollUntilFound(() ->
                    streamStatusFinder.findStreamStatus(streamErrorDetails.streamId(), "cakeshop", "EVENT_LISTENER"));
            if (streamStatus.isPresent()) {
                assertThat(streamStatus.get().streamErrorId(), is(streamErrorDetails.id()));
                assertThat(streamStatus.get().streamErrorPosition(), is(streamErrorDetails.positionInStream()));
                assertThat(streamStatus.get().streamId(), is(streamErrorDetails.streamId()));

            } else {
                fail("Could not find stream status for streamId: " + streamErrorDetails.streamId());
            }
        } else {
            fail("Failed to find stream error for event named '" + eventName + "' in stream_error table");
        }
    }
}

