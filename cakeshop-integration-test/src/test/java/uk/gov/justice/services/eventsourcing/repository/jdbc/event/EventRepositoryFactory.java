package uk.gov.justice.services.eventsourcing.repository.jdbc.event;

import static org.slf4j.LoggerFactory.getLogger;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import uk.gov.justice.services.eventsourcing.repository.jdbc.EventInsertionStrategy;
import uk.gov.justice.services.jdbc.persistence.JdbcResultSetStreamer;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;
import uk.gov.justice.services.test.utils.persistence.SettableEventStoreDataSourceProvider;

import javax.sql.DataSource;

public class EventRepositoryFactory {

    public EventJdbcRepository getEventJdbcRepository(final DataSource dataSource) {

        final EventJdbcRepository eventJdbcRepository = new EventJdbcRepository();

        final SettableEventStoreDataSourceProvider eventStoreDataSourceProvider = new SettableEventStoreDataSourceProvider();
        eventStoreDataSourceProvider.setDataSource(dataSource);

        setField(eventJdbcRepository, "eventInsertionStrategy", new EventInsertionStrategy());
        setField(eventJdbcRepository, "jdbcResultSetStreamer", new JdbcResultSetStreamer());
        setField(eventJdbcRepository, "preparedStatementWrapperFactory", new PreparedStatementWrapperFactory());
        setField(eventJdbcRepository, "eventStoreDataSourceProvider", eventStoreDataSourceProvider);
        setField(eventJdbcRepository, "logger", getLogger(EventJdbcRepository.class));

        return eventJdbcRepository;
    }
}
