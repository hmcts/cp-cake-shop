package uk.gov.justice.services.cakeshop.it;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.jupiter.api.Assertions.assertNull;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_URI;

import uk.gov.justice.services.cakeshop.it.helpers.EventFactory;
import uk.gov.justice.services.cakeshop.it.helpers.JmsBootstrapper;
import uk.gov.justice.services.cakeshop.it.helpers.Querier;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.test.utils.core.messaging.Poller;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.ws.rs.client.Client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.extension.ExtendWith;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseResetExtension;

@ExtendWith(DatabaseResetExtension.class)
public class CakeShopManyUpdatesIT {

    private static final String COMMAND_HANDLER_QUEUE = "cakeshop.handler.command";
    private static final String DEAD_LETTER_QUEUE = "DLQ";

    private final JmsBootstrapper jmsBootstrapper = new JmsBootstrapper();
    private final EventFactory eventFactory = new EventFactory();
    private Querier querier;

    private Client client;

    @BeforeEach
    public void before() throws Exception {
        client = new RestEasyClientFactory().createResteasyClient();
        querier = new Querier(client);
    }

    @AfterEach
    public void cleanup() throws Exception {
        client.close();
    }

    @Test
    public void shouldSuccessfullyProcessManyUpdatesToSameRecipeId() throws Exception {
        jmsBootstrapper.clearDeadLetterQueue();

        final String recipeId = randomUUID().toString();
        final String recipeName = "Original Cheese Cake";

        client.target(RECIPES_RESOURCE_URI + recipeId)
                .request()
                .post(eventFactory.recipeEntity(recipeName));

        new Poller().pollUntilFound(() -> {
            if (querier.queryForRecipe(recipeId).httpCode() == OK.getStatusCode()) {
                return of(true);
            }

            return empty();
        });

        // Do many renames
        int updateCount = 10;
        for (int i = 0; i < updateCount; i++) {
            //random generator string
            client.target(RECIPES_RESOURCE_URI + recipeId)
                    .request()
                    .put(eventFactory.renameRecipeEntity("New Name"));
        }

        try (final Session jmsSession = jmsBootstrapper.jmsSession()) {

            final QueueBrowser queueBrowser = jmsBootstrapper.queueBrowserOf(COMMAND_HANDLER_QUEUE, jmsSession);

            new Poller().pollUntilNotFound(() -> {
                try {
                    if (queueBrowser.getEnumeration().hasMoreElements()) {
                        return of(true);
                    }
                } catch (JMSException e) {
                    System.out.println("Browsing Queue failed");
                    throw new RuntimeException(e);
                }

                return empty();
            });

            final MessageConsumer dlqConsumer = jmsBootstrapper.queueConsumerOf(DEAD_LETTER_QUEUE, jmsSession);
            final Message message = dlqConsumer.receiveNoWait();

            assertNull(message, "Dead letter queue is not empty, found message: ");
        }

        client.target(RECIPES_RESOURCE_URI + recipeId).request()
                .put(eventFactory.renameRecipeEntity("Final Name"));

        new Poller().pollUntilFound(() -> {
            System.out.printf("Polling for query response body to contain 'Final Name' for recipeId: %s\n", recipeId);
            if (querier.queryForRecipe(recipeId).body().contains("Final Name")) {
                return of(true);
            }

            return empty();
        });
    }
}
