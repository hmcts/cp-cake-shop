package uk.gov.justice.services.cakeshop.it.helpers;

import uk.gov.justice.services.common.converter.ZonedDateTimes;

import static java.util.Optional.empty;
import static java.util.Optional.of;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

public class StreamStatusFinder {

    private final DataSource viewStoreDataSource;

    public StreamStatusFinder(DataSource viewStoreDataSource) {
        this.viewStoreDataSource = viewStoreDataSource;
    }

    public Optional<StreamStatus> findStreamStatus(final UUID streamId, final String source, final String component) {

        final String SELECT_SQL = """
                    SELECT
                    stream_id,
                    position, 
                    source,
                    component,
                    stream_error_id,
                    stream_error_position, 
                    updated_at, 
                    latest_known_position, 
                    is_up_to_date
                FROM stream_status
                WHERE stream_id = ?
                AND source = ?
                AND component = ?""";

        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_SQL)) {
            preparedStatement.setObject(1, streamId);
            preparedStatement.setString(2, source);
            preparedStatement.setString(3, component);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final StreamStatus streamStatus = new StreamStatus(
                            (UUID) resultSet.getObject("stream_id"),
                            resultSet.getLong("position"),
                            resultSet.getString("source"),
                            resultSet.getString("component"),
                            (UUID) resultSet.getObject("stream_error_id"),
                            resultSet.getLong("stream_error_position"),
                            ZonedDateTimes.fromSqlTimestamp(resultSet.getTimestamp("updated_at")),
                            resultSet.getLong("latest_known_position"),
                            resultSet.getBoolean("is_up_to_date"));

                    return of(streamStatus);
                }
            }
        } catch (final SQLException e) {
            throw new RuntimeException("Failed to read from stream status table", e);
        }

        return empty();
    }

    public record StreamStatus(UUID streamId, long position, String source, String component, UUID streamErrorId, Long streamErrorPosition, ZonedDateTime updatedAt, long latestKnownPosition, boolean isUpToDate) {

    }
}
