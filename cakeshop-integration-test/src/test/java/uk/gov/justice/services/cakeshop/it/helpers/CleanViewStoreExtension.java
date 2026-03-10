package uk.gov.justice.services.cakeshop.it.helpers;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;

public class CleanViewStoreExtension implements BeforeEachCallback {

    private final CakeShopDatabaseCleaner cakeShopDatabaseCleaner = new CakeShopDatabaseCleaner();
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

    @Override
    public void beforeEach(final ExtensionContext context) {
        cakeShopDatabaseCleaner.cleanViewStoreTables();
        databaseCleaner.resetEventSubscriptionStatusTable(CONTEXT_NAME);
        databaseCleaner.cleanEventStoreTables(CONTEXT_NAME);
    }
}
