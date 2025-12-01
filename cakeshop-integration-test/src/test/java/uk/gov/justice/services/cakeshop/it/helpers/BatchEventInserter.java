package uk.gov.justice.services.cakeshop.it.helpers;

import static java.lang.String.format;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

public class BatchEventInserter {

    private static final String SQL_INSERT_EVENT =
            "INSERT INTO event_log " +
            "(id, stream_id, position_in_stream, name, metadata, payload, date_created, event_number, previous_event_number) " +
            "VALUES(?, ?, ?, ?, ?, ?, ?, NULL, NULL)";

    private static final String SQL_INSERT_STREAM = "INSERT INTO event_stream " +
                                                    "(stream_id, date_created, active) values (?, ?, ?)";


    private final DataSource eventStoreDataSource;
    private final int batchSize;

    private final UtcClock clock = new UtcClock();

    public BatchEventInserter(final DataSource eventStoreDataSource, final int batchSize) {
        this.eventStoreDataSource = eventStoreDataSource;
        this.batchSize = batchSize;
    }

    public void updateEventLogTable(final List<Event> events) throws Exception {

        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SQL_INSERT_EVENT)) {
            for (int i = 0; i < events.size(); i++) {

                final Event event = events.get(i);
                preparedStatement.setObject(1, event.getId());
                preparedStatement.setObject(2, event.getStreamId());
                preparedStatement.setLong(3, event.getPositionInStream());
                preparedStatement.setString(4, event.getName());
                preparedStatement.setString(5, event.getMetadata());
                preparedStatement.setString(6, event.getPayload());
                preparedStatement.setTimestamp(7, toSqlTimestamp(event.getCreatedAt()));

                preparedStatement.addBatch();

                if (i > 0 && i % batchSize == 0) {
                    preparedStatement.executeBatch();
                    System.out.println(format("Inserted %d events into event_log...", i));
                }
            }

            preparedStatement.executeBatch();
        }
    }

    public void updateEventStreamTable(final List<UUID> streamIds) throws Exception {

        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SQL_INSERT_STREAM)) {
            for (int i = 0; i < streamIds.size(); i++) {
                final UUID streamId = streamIds.get(i);
                preparedStatement.setObject(1, streamId);
                preparedStatement.setTimestamp(2, toSqlTimestamp(clock.now()));
                preparedStatement.setBoolean(3, true);

                preparedStatement.addBatch();

                if (i % batchSize == 0) {
                    preparedStatement.executeBatch();
                }
            }

            preparedStatement.executeBatch();
        }
    }
}