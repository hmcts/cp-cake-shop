package uk.gov.justice.services.cakeshop.it.helpers;

import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;

public class CakeShopDatabaseCleaner {

    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

    public void cleanViewStoreTables() {
        databaseCleaner.cleanViewStoreTables(
                CONTEXT_NAME,
                "stream_buffer",
                "stream_status",
                "stream_error_hash",
                "stream_error",
                "stream_error_retry",
                "cake",
                "cake_order",
                "recipe",
                "ingredient",
                "processed_event");

    }
}
