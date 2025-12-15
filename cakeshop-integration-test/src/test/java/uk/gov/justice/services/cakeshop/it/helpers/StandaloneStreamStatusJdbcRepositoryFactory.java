package uk.gov.justice.services.cakeshop.it.helpers;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatusJdbcRepository;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import javax.sql.DataSource;

public class StandaloneStreamStatusJdbcRepositoryFactory {

    public StreamStatusJdbcRepository getStreamStatusJdbcRepository(final DataSource dataSource) {

        final ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider = new ViewStoreJdbcDataSourceProvider() {
            @Override
            public synchronized DataSource getDataSource() {
                return new DatabaseManager().initViewStoreDb();
            }
        };

        return new StreamStatusJdbcRepository(
                viewStoreJdbcDataSourceProvider,
                new PreparedStatementWrapperFactory(),
                new UtcClock());
    }
}
