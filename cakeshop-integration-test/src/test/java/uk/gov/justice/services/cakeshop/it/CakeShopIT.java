package uk.gov.justice.services.cakeshop.it;

import static com.jayway.jsonassert.JsonAssert.with;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.OK;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopMediaTypes.ADD_RECIPE_MEDIA_TYPE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopMediaTypes.REMOVE_RECIPE_MEDIA_TYPE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.OVEN_RESOURCE_CUSTOM_URI;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_URI;
import static uk.gov.justice.services.test.utils.core.matchers.HttpStatusCodeMatcher.isStatus;

import uk.gov.justice.services.cakeshop.it.helpers.ApiResponse;
import uk.gov.justice.services.cakeshop.it.helpers.CommandFactory;
import uk.gov.justice.services.cakeshop.it.helpers.CommandSender;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.EventFactory;
import uk.gov.justice.services.cakeshop.it.helpers.EventFinder;
import uk.gov.justice.services.cakeshop.it.helpers.JmsBootstrapper;
import uk.gov.justice.services.cakeshop.it.helpers.Querier;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.common.converter.ObjectToJsonObjectConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventRepositoryFactory;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import java.util.Optional;

import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.sql.DataSource;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.message.BasicNameValuePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CakeShopIT {

    private final DataSource eventStoreDataSource = new DatabaseManager().initEventStoreDb();
    private final EventJdbcRepository eventJdbcRepository = new EventRepositoryFactory().getEventJdbcRepository(eventStoreDataSource);
    private final ObjectToJsonObjectConverter objectToJsonObjectConverter = new ObjectToJsonObjectConverter(new ObjectMapper());
    private final JmsBootstrapper jmsBootstrapper = new JmsBootstrapper();

    private final EventFactory eventFactory = new EventFactory();
    private final EventFinder eventFinder = new EventFinder(eventJdbcRepository);
    private final CommandFactory commandFactory = new CommandFactory();
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

    private Client client;
    private Querier querier;
    private CommandSender commandSender;

    private static final int MAX_POLL_TIME_IN_SECONDS = 20;

    @BeforeEach
    public void before() throws Exception {
        client = new RestEasyClientFactory().createResteasyClient();
        querier = new Querier(client);
        commandSender = new CommandSender(client, eventFactory);

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
    public void shouldAcceptAddRecipeCommand() throws Exception {
        final String recipeId = randomUUID().toString();

        final Response response = client
                .target(RECIPES_RESOURCE_URI + recipeId)
                .request()
                .post(entity(commandFactory.addRecipeCommand(), ADD_RECIPE_MEDIA_TYPE));

        assertThat(response.getStatus(), isStatus(ACCEPTED));
    }

    @Test
    public void shouldSendNotificationThroughJobStoreTaskOnProcessingCakeMadeEvent() throws Exception {
        try (final Session jmsSession = jmsBootstrapper.jmsSession()) {
            try (final MessageConsumer publicTopicConsumer = jmsBootstrapper.topicConsumerOf("public.event", jmsSession)) {
                jmsBootstrapper.clear(publicTopicConsumer);

                final String recipeId = randomUUID().toString();
                final String cakeId = randomUUID().toString();
                final String cakeName = "Super cake";

                commandSender.addRecipe(recipeId, cakeName);
                await().atMost(ofSeconds(MAX_POLL_TIME_IN_SECONDS)).until(() -> querier.recipesQueryResult().body().contains(recipeId));
                final ApiResponse apiResponse = commandSender.makeCake(recipeId, cakeId);
                assertThat(apiResponse.httpCode(), isStatus(ACCEPTED));

                final Optional<String> eventPayload = jmsBootstrapper.getPayloadByEventName(publicTopicConsumer, "jobstore.task.notification.cake-made");
                assertTrue(eventPayload.isPresent());
                with(eventPayload.get())
                        .assertThat("$.cakeId", equalTo(cakeId));
            }
        }
    }

    @Test
    public void shouldAcceptRemoveRecipeCommand() throws Exception {
        final String recipeId = randomUUID().toString();
        final Response response_1 = client
                .target(RECIPES_RESOURCE_URI + recipeId)
                .request()
                .post(entity(commandFactory.addRecipeCommand(), ADD_RECIPE_MEDIA_TYPE));

        assertThat(response_1.getStatus(), isStatus(ACCEPTED));

        await().atMost(ofSeconds(MAX_POLL_TIME_IN_SECONDS)).until(() -> eventFinder.eventsWithPayloadContaining(recipeId).size() == 1);

        final Response response_2 = client
                .target(RECIPES_RESOURCE_URI + recipeId)
                .request()
                .post(entity(commandFactory.addRecipeCommand(), REMOVE_RECIPE_MEDIA_TYPE));

        assertThat(response_2.getStatus(), isStatus(ACCEPTED));
    }

    @Test
    public void shouldProcessRecipeAddedEvent() {
        final String recipeId = randomUUID().toString();
        final String eventJson = createObjectBuilder()
                .add("name", "Vanilla cake")
                .add("glutenFree", false)
                .add("ingredients", createArrayBuilder()
                        .add(createObjectBuilder()
                                .add("name", "vanilla")
                                .add("quantity", 2)
                        ).build()
                ).build()
                .toString();

        client.target(RECIPES_RESOURCE_URI + recipeId)
                .request()
                .post(entity(
                        eventJson,
                        ADD_RECIPE_MEDIA_TYPE));

        await().atMost(ofSeconds(MAX_POLL_TIME_IN_SECONDS)).until(() -> eventFinder.eventsWithPayloadContaining(recipeId).size() == 1);

        final Event event = eventFinder.eventsWithPayloadContaining(recipeId).get(0);
        assertThat(event.getName(), is("cakeshop.events.recipe-added"));
        with(event.getMetadata())
                .assertEquals("stream.id", recipeId)
                .assertEquals("stream.version", 1);
        final String eventPayload = event.getPayload();
        with(eventPayload)
                .assertThat("$.recipeId", equalTo(recipeId))
                .assertThat("$.name", equalTo("Vanilla cake"))
                .assertThat("$.glutenFree", equalTo(false))
                .assertThat("$.ingredients[0].name", equalTo("vanilla"))
                .assertThat("$.ingredients[0].quantity", equalTo(2));
    }

    @Test
    public void shouldQueryRecipesById() {
        final String recipeId = randomUUID().toString();
        final String recipeName = "Cheesy cheese cake";
        commandSender.addRecipe(recipeId, recipeName);

        await().atMost(ofSeconds(MAX_POLL_TIME_IN_SECONDS)).until(() -> querier.queryForRecipe(recipeId).httpCode() == OK.getStatusCode());

        final ApiResponse response = querier.queryForRecipe(recipeId);

        with(response.body())
                .assertThat("$.id", equalTo(recipeId))
                .assertThat("$.name", equalTo(recipeName));
    }

    @Test
    public void shouldQueryForRecipes() {
        //adding 2 recipes
        final String recipeId = randomUUID().toString();
        commandSender.addRecipe(recipeId, "Cheesy cheese cake");

        final String recipeId2 = randomUUID().toString();
        commandSender.addRecipe(recipeId2, "Chocolate muffin");


        await().atMost(ofSeconds(MAX_POLL_TIME_IN_SECONDS)).until(() -> {
            final String responseBody = querier.recipesQueryResult(singletonList(new BasicNameValuePair("pagesize", "30"))).body();
            return responseBody.contains(recipeId) && responseBody.contains(recipeId2);
        });

        final ApiResponse response = querier.recipesQueryResult();
        assertThat(response.httpCode(), isStatus(OK));

        with(response.body())
                .assertThat("$.recipes[?(@.id=='" + recipeId + "')].name", hasItem("Cheesy cheese cake"))
                .assertThat("$.recipes[?(@.id=='" + recipeId2 + "')].name", hasItem("Chocolate muffin"));
    }

    @Test
    public void shouldReturnStatusFromMakeCakeAction() throws Exception {
        final String recipeId = randomUUID().toString();
        final String cakeId = randomUUID().toString();
        final String cakeName = "Super cake";

        commandSender.addRecipe(recipeId, cakeName);
        commandSender.addRecipe(randomUUID().toString(), "cake");
        await().atMost(ofSeconds(MAX_POLL_TIME_IN_SECONDS)).until(() -> querier.recipesQueryResult().body().contains(recipeId));

        final ApiResponse apiResponse = commandSender.makeCake(recipeId, cakeId);

        with(apiResponse.body())
                .assertThat("$.status", equalTo("Making Cake"));
    }

    @Test
    public void shouldQueryForCakesWithNamesInheritedFromRecipe() throws Exception {
        final String recipeId_1 = randomUUID().toString();
        final String recipeId_2 = randomUUID().toString();

        final String cakeId = randomUUID().toString();
        final String cakeName = "Super cake";

        commandSender.addRecipe(recipeId_1, cakeName);
        commandSender.addRecipe(recipeId_2, "cake");
        await().atMost(ofSeconds(MAX_POLL_TIME_IN_SECONDS)).until(() -> querier.recipesQueryResult().body().contains(recipeId_1));

        commandSender.makeCake(recipeId_1, cakeId);

        await().atMost(ofSeconds(MAX_POLL_TIME_IN_SECONDS)).until(() -> querier.cakesQueryResult().body().contains(cakeId));

        //slightly contrived domain logic: when a cake is made, it gets a name of the recipe
        with(querier.cakesQueryResult().body())
                .assertThat("$.cakes[?(@.id == '" + cakeId + "')].name", hasItem(cakeName));
    }

    @Test
    public void shouldUpdateRecipeWithNewName() throws Exception {
        final String recipeId = randomUUID().toString();
        final String recipeName = "Original Cheese Cake";

        client.target(RECIPES_RESOURCE_URI + recipeId).request()
                .post(eventFactory.recipeEntity(recipeName, false));

        await().atMost(ofSeconds(MAX_POLL_TIME_IN_SECONDS)).until(() -> querier.queryForRecipe(recipeId).httpCode() == OK.getStatusCode());

        client.target(RECIPES_RESOURCE_URI + recipeId).request()
                .put(eventFactory.renameRecipeEntity("New Name"));

        await().atMost(ofSeconds(MAX_POLL_TIME_IN_SECONDS)).until(() -> querier.queryForRecipe(recipeId).body().contains("New Name"));
    }

    @Test
    public void shouldQueryForOvenStatus() throws Exception {
        final Response response = client.target(OVEN_RESOURCE_CUSTOM_URI).request().accept("application/vnd.cakeshop.status+json").get();
        assertThat(response.getStatus(), isStatus(OK));

        final String entity = response.readEntity(String.class);

        with(entity)
                .assertEquals("$.ovens[0].name", "Big Oven")
                .assertEquals("$.ovens[1].name", "Large Oven");
    }
}
