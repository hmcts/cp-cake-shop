package uk.gov.justice.services.cakeshop;

import static com.jayway.jsonassert.JsonAssert.with;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.HOST;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_URI;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;

import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.EventFactory;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrence;
import uk.gov.justice.services.test.utils.core.messaging.Poller;

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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class ResetEventPublishingRetriesIT {

    protected static final String RESET_RETRY_COUNT_URI_PATTERN = "%s/cakeshop-service/internal/reset-stream-retry-count?streamId=%s&source=%s&component=%s";
    private final DataSource viewStoreDataSource = new DatabaseManager().initViewStoreDb();

    private final Client restEastClient = new RestEasyClientFactory().createResteasyClient();;
    private final Poller poller = new Poller(20, 1000L);;

    @AfterEach
    public void cleanup() throws Exception {
        dropFailureTrigger();
        restEastClient.close();
    }

    @Test
    public void shouldSendResetRetryCountForFailedEventAndThenSuccessfullyProcessEvent() throws Exception {

        createFailureTrigger();

        final String eventName = "cakeshop.events.recipe-added";
        final String recipeName = "Deep Fried Mars Bar";
        final UUID recipeId = randomUUID();
        final String source = "cakeshop";
        final String component = "EVENT_LISTENER";

        sendAddRecipeCommand(recipeName, recipeId);

        final Optional<StreamError> streamError = poller.pollUntilFound(
                () -> findEventListenerStreamError(eventName)
        );

        if (streamError.isEmpty()) {
            fail("No failed event found in stream_error tables");
        }

        dropFailureTrigger();

        final String resetRetryCountUri = RESET_RETRY_COUNT_URI_PATTERN.formatted(
                HOST,
                recipeId,
                source,
                component);

        final Invocation.Builder request = restEastClient.target(resetRetryCountUri).request();
        try (final Response response = request.get()) {
            assertThat(response.getStatus(), is(200));

            final String json = response.readEntity(String.class);

            with(json)
                    .assertThat("$.success", is(true))
                    .assertThat("$.message", is("Stream retry count reset to zero"))
                    .assertThat("$.streamId", is(recipeId.toString()))
                    .assertThat("$.source", is(source))
                    .assertThat("$.component", is(component));
        }

        poller.pollUntilNotFound(() -> findEventListenerStreamError(eventName));
    }

    @Test
    public void shouldReturn400BadRequestIfStreamNotFoundInRetryTable() throws Exception {

        final UUID recipeId = fromString("bdc8c193-195a-4d45-8d0a-15c4abb366a0");
        final String source = "cakeshop";
        final String component = "EVENT_LISTENER";

        final String resetRetryCountUri = RESET_RETRY_COUNT_URI_PATTERN.formatted(
                HOST,
                recipeId,
                source,
                component);

        final Invocation.Builder request = restEastClient.target(resetRetryCountUri).request();
        try (final Response response = request.get()) {
            assertThat(response.getStatus(), is(400));

            final String json = response.readEntity(String.class);

            with(json)
                    .assertThat("$.success", is(false))
                    .assertThat("$.message", is("Failed to reset stream retry count. No stream retry found for streamId: 'bdc8c193-195a-4d45-8d0a-15c4abb366a0', source: 'cakeshop', component: 'EVENT_LISTENER'"))
                    .assertThat("$.streamId", is(recipeId.toString()))
                    .assertThat("$.source", is(source))
                    .assertThat("$.component", is(component));
        }
    }

    private void createFailureTrigger() throws SQLException {

        final String triggerSql = """
                CREATE OR REPLACE TRIGGER recipe_error_trigger
                    BEFORE INSERT OR UPDATE
                    ON recipe
                    FOR EACH ROW
                EXECUTE FUNCTION throw_error_trigger_simple();
                """;
        final String functionSql = """
                create or replace function throw_error_trigger_simple() returns trigger language plpgsql
                as
                $$
                BEGIN
                   RAISE EXCEPTION 'Deliberately failing insert for testing' USING ERRCODE = 'P0002';
                   RETURN NEW;
                END;
                $$;
                """;
        try (final Connection connection = viewStoreDataSource.getConnection()) {
            try(final PreparedStatement preparedStatement = connection.prepareStatement(functionSql)) {
                preparedStatement.execute();
            }
            try(final PreparedStatement preparedStatement = connection.prepareStatement(triggerSql)) {
                preparedStatement.execute();
            }
        }
    }

    private void dropFailureTrigger() throws SQLException {
        try (final Connection connection = viewStoreDataSource.getConnection()) {
            try(final PreparedStatement preparedStatement = connection.prepareStatement("DROP TRIGGER IF EXISTS recipe_error_trigger ON recipe;")) {
                preparedStatement.execute();
            }
            try(final PreparedStatement preparedStatement = connection.prepareStatement("DROP FUNCTION IF EXISTS throw_error_trigger_simple;")) {
                preparedStatement.execute();
            }
        }
    }

    private void sendAddRecipeCommand(final String recipeName, final UUID recipeId) {
        final EventFactory eventFactory = new EventFactory();
        final Entity<String> recipeEntity = eventFactory.recipeEntity(recipeName, false);
        final Invocation.Builder request = restEastClient.target(RECIPES_RESOURCE_URI + recipeId).request();
        try (final Response response = request.post(recipeEntity)) {
            assertThat(response.getStatus(), is(202));
        }
    }

    private Optional<StreamError> findEventListenerStreamError(final String eventName) {

        final Optional<StreamErrorOccurrence> streamErrorOccurrence = findStreamError(eventName);

        if (streamErrorOccurrence.isPresent()) {
            final Optional<StreamErrorHash> streamErrorHash = findStreamErrorHash(streamErrorOccurrence.get().hash());

            if (streamErrorHash.isPresent()) {
                return of(new StreamError(streamErrorOccurrence.get(), streamErrorHash.get()));
            }
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
        } catch (final SQLException e) {
            throw new RuntimeException("Failed to read from stream_error table", e);
        }
    }

    private Optional<StreamErrorOccurrence> findStreamError(final String eventName) {
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
                    source,
                    occurred_at
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
                    final ZonedDateTime dateCreated = fromSqlTimestamp(resultSet.getTimestamp("date_created"));
                    final String stackTrace = resultSet.getString("full_stack_trace");
                    final String componentName = resultSet.getString("component");
                    final String source = resultSet.getString("source");
                    final ZonedDateTime occurredAt = fromSqlTimestamp(resultSet.getTimestamp("occurred_at"));

                    final StreamErrorOccurrence streamError = new StreamErrorOccurrence(
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
                            source,
                            occurredAt
                    );

                    return of(streamError);
                }
            }
        } catch (final SQLException e) {
            throw new RuntimeException("Failed to read from stream error table", e);
        }

        return empty();
    }
}
