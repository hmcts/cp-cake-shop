package uk.gov.justice.services.cakeshop.it.helpers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

import static java.util.Optional.empty;
import static java.util.Optional.of;

public class StreamStatusFinder {

    private final DataSource viewStoreDataSource;

    public StreamStatusFinder(DataSource viewStoreDataSource) {
        this.viewStoreDataSource = viewStoreDataSource;
    }

    public Optional<StreamStatus> findStreamStatus(final UUID streamId, final String source, final String component) {

        final String SELECT_SQL = """
                    SELECT
                    stream_id,
                    stream_error_id,
                    stream_error_position
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
                            (UUID) resultSet.getObject("stream_error_id"),
                            resultSet.getLong("stream_error_position")
                    );

                    return of(streamStatus);
                }
            }
        } catch (final SQLException e) {
            throw new RuntimeException("Failed to read from stream status table", e);
        }

        return empty();
    }

    public record StreamStatus(UUID streamId, UUID streamErrorId, Long streamErrorPosition) {

    }
}
