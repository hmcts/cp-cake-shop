package uk.gov.justice.services.cakeshop.it;

import static java.time.Duration.ofSeconds;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.client.Entity.entity;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopMediaTypes.ADD_RECIPE_MEDIA_TYPE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_URI;

import uk.gov.justice.services.cakeshop.it.helpers.CommandFactory;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.cakeshop.it.helpers.StandaloneStreamStatusJdbcRepositoryFactory;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatusJdbcRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.Subscription;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import java.util.Optional;

import javax.sql.DataSource;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CakeShopEventBufferingIT {

    private final DataSource viewStoreDatasource = new DatabaseManager().initViewStoreDb();
    private final StandaloneStreamStatusJdbcRepositoryFactory standaloneStreamStatusJdbcRepositoryFactory = new StandaloneStreamStatusJdbcRepositoryFactory();
    private final StreamStatusJdbcRepository streamStatusJdbcRepository = standaloneStreamStatusJdbcRepositoryFactory.getStreamStatusJdbcRepository(viewStoreDatasource);

    private final CommandFactory commandFactory = new CommandFactory();
    final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

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

    @Test
    public void shouldUpdateEventBufferStatus() throws Exception {
        final String recipeId = randomUUID().toString();

        final Entity<String> entity = entity(commandFactory.addRecipeCommand(), ADD_RECIPE_MEDIA_TYPE);

        try(final Response response = client.target(RECIPES_RESOURCE_URI + recipeId)
                .request()
                .post(entity)) {
            assertThat(response.getStatus(), is(202));
        }

        await().atMost(ofSeconds(20)).until(() -> subscription(recipeId).isPresent());
        assertThat(subscription(recipeId).get().getPosition(), is(1L));
    }

    @SuppressWarnings("SameParameterValue")
    private Optional<Subscription> subscription(final String recipeId) {
        return streamStatusJdbcRepository.findByStreamIdAndSource(fromString(recipeId), CONTEXT_NAME, "EVENT_LISTENER");
    }
}
