package uk.gov.justice.services.cakeshop.it.helpers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import uk.gov.justice.services.common.converter.ZonedDateTimes;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.test.utils.core.messaging.Poller;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_URI;

public class TestDataManager {

    private final DataSource viewStoreDataSource;
    private final EventFactory eventFactory;
    private final Poller poller;

    public TestDataManager(DataSource viewStoreDataSource) {
        this.viewStoreDataSource = viewStoreDataSource;
        this.eventFactory = new EventFactory();
        this.poller = new Poller(20, 1000L);
    }

    public Optional<StreamError> createAnEventWithViewStoreFailure() {
        return createAnEventWithViewStoreFailure("cakeshop.events.recipe-added");
    }

    public Optional<StreamError> createAnEventWithViewStoreFailure(String eventName) {
        final Client client = new RestEasyClientFactory().createResteasyClient();
        final String recipeId = "6a710473-2af4-44a9-99f1-4c27632d5b23";
        final String recipeName = "DELIBERATELY_FAIL";

        final Entity<String> recipeEntity = eventFactory.recipeEntity(recipeName, false);
        final Invocation.Builder request = client.target(RECIPES_RESOURCE_URI + recipeId).request();
        try (final Response response = request.post(recipeEntity)) {
            assertThat(response.getStatus(), is(202));
        }

        client.close();

        return poller.pollUntilFound(() -> findEventListenerStreamError(eventName));
    }



    private Optional<StreamError> findEventListenerStreamError(final String eventName) {

        final Optional<StreamErrorDetails> streamErrorDetails = findStreamErrorDetails(eventName);

        if (streamErrorDetails.isPresent()) {
            final Optional<StreamErrorHash> streamErrorHash = findStreamErrorHash(streamErrorDetails.get().hash());

            if (streamErrorHash.isPresent()) {
                return of(new StreamError(streamErrorDetails.get(), streamErrorHash.get()));
            }
        }

        return empty();
    }

    private Optional<StreamErrorDetails> findStreamErrorDetails(final String eventName) {
        final String SELECT_SQL = """
                    SELECT
                    id,
                    hash,
                    exception_message,
                    cause_message,
                    event_id,
                    stream_id,
                    position_in_stream,
                    date_created,
                    full_stack_trace,
                    component,
                    source
                FROM stream_error
                WHERE event_name = ? AND component = 'EVENT_LISTENER'""";

        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_SQL)) {
            preparedStatement.setString(1, eventName);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final UUID id = (UUID) resultSet.getObject("id");
                    final String hash = resultSet.getString("hash");
                    final String exceptionMessage = resultSet.getString("exception_message");
                    final Optional<String> causeMessage = ofNullable(resultSet.getString("cause_message"));
                    final UUID eventId = (UUID) resultSet.getObject("event_id");
                    final UUID streamId = (UUID) resultSet.getObject("stream_id");
                    final Long positionInStream = resultSet.getLong("position_in_stream");
                    final ZonedDateTime dateCreated = ZonedDateTimes.fromSqlTimestamp(resultSet.getTimestamp("date_created"));
                    final String stackTrace = resultSet.getString("full_stack_trace");
                    final String componentName = resultSet.getString("component");
                    final String source = resultSet.getString("source");

                    final StreamErrorDetails streamError = new StreamErrorDetails(
                            id,
                            hash,
                            exceptionMessage,
                            causeMessage,
                            eventName,
                            eventId,
                            streamId,
                            positionInStream,
                            dateCreated,
                            stackTrace,
                            componentName,
                            source
                    );

                    return of(streamError);
                }
            }
        } catch (final SQLException e) {
            throw new RuntimeException("Failed to read from stream error table", e);
        }

        return empty();
    }

    private Optional<StreamErrorHash> findStreamErrorHash(final String hash) {

        final String SELECT_SQL = """
                        SELECT
                            exception_classname,
                            cause_classname,
                            java_classname,
                            java_method,
                            java_line_number
                        FROM stream_error_hash
                        WHERE hash = ?
                """;

        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_SQL)) {
            preparedStatement.setString(1, hash);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final String exceptionClassname = resultSet.getString("exception_classname");
                    final Optional<String> causeClassname = ofNullable(resultSet.getString("cause_classname"));
                    final String javaClassname = resultSet.getString("java_classname");
                    final String javaMethod = resultSet.getString("java_method");
                    final int javaLineNumber = resultSet.getInt("java_line_number");

                    final StreamErrorHash streamErrorHash = new StreamErrorHash(
                            hash,
                            exceptionClassname,
                            causeClassname,
                            javaClassname,
                            javaMethod,
                            javaLineNumber
                    );

                    return of(streamErrorHash);
                }

                return empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read from stream_error table", e);
        }
    }
}
