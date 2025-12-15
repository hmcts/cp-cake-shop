package uk.gov.justice.services.cakeshop.it;

import static com.google.common.io.Resources.getResource;
import static com.jayway.jsonassert.JsonAssert.with;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.apache.commons.io.IOUtils.contentEquals;
import static org.apache.http.entity.ContentType.APPLICATION_OCTET_STREAM;
import static org.apache.http.entity.mime.HttpMultipartMode.BROWSER_COMPATIBLE;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_QUERY_URI;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_URI;
import static uk.gov.justice.services.test.utils.core.matchers.HttpStatusCodeMatcher.isStatus;

import uk.gov.justice.services.cakeshop.it.helpers.CommandSender;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.EventFactory;
import uk.gov.justice.services.cakeshop.it.helpers.EventFinder;
import uk.gov.justice.services.cakeshop.it.helpers.Querier;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventRepositoryFactory;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.sql.DataSource;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class CakeShopFileServiceIT {

    private final DataSource eventStoreDataSource = new DatabaseManager().initEventStoreDb();
    private final EventJdbcRepository eventJdbcRepository = new EventRepositoryFactory().getEventJdbcRepository(eventStoreDataSource);

    private final EventFinder eventFinder = new EventFinder(eventJdbcRepository);

    private Client client;
    private Querier querier;
    private CommandSender commandSender;

    @BeforeEach
    public void before() throws Exception {
        new DatabaseCleaner().cleanViewStoreTables(CONTEXT_NAME, "cake", "cake_order", "recipe", "ingredient", "processed_event");
        client = new RestEasyClientFactory().createResteasyClient();
        querier = new Querier(client);
        commandSender = new CommandSender(client, new EventFactory());
    }

    @AfterEach
    public void cleanup() throws Exception {
        client.close();
    }

    public void shouldReturnAcceptedStatusAndCreatEventWhenPostingPhotographToMultipartEndpoint() throws Exception {

        final String recipeId = randomUUID().toString();
        final String fieldName = "photoId";
        final String filename = "croydon.jpg";
        final File file = new File(getResource(filename).getFile());

        commandSender.addRecipe(recipeId, "Cheesy cheese cake");

        await().until(() -> querier.queryForRecipe(recipeId).httpCode() == OK.getStatusCode());

        final HttpEntity httpEntity = MultipartEntityBuilder.create()
                .setMode(BROWSER_COMPATIBLE)
                .addBinaryBody(fieldName, file, APPLICATION_OCTET_STREAM, filename)
                .build();

        final HttpPost request = new HttpPost(RECIPES_RESOURCE_URI + recipeId + "/photograph");
        request.setEntity(httpEntity);

        final HttpResponse response = HttpClients.createDefault().execute(request);

        assertThat(response.getStatusLine().getStatusCode(), isStatus(ACCEPTED));

        await().until(() -> eventFinder.eventsWithPayloadContaining(recipeId).size() == 2);

        final Event event = eventFinder.eventsWithPayloadContaining(recipeId).get(1);
        assertThat(event.getName(), is("cakeshop.events.recipe-photograph-added"));
        with(event.getMetadata())
                .assertEquals("stream.id", recipeId)
                .assertEquals("stream.version", 2);
        with(event.getPayload())
                .assertThat("$.recipeId", equalTo(recipeId))
                .assertThat("$.photoId", notNullValue());
    }

    public void shouldRetrieveRecipePhotograph() throws Exception {
        final String recipeId = randomUUID().toString();
        commandSender.addRecipe(recipeId, "Easy Muffin");
        await().until(() -> querier.queryForRecipe(recipeId).httpCode() == OK.getStatusCode());

        assertThat(photographFor(recipeId).get().getStatus(), isStatus(NOT_FOUND));

        final String filename = "croydon.jpg";
        appendFileToTheRecipe(recipeId, filename);

        await().until(() -> photographFor(recipeId).get().getStatus() == OK.getStatusCode());

        final InputStream returnedStream = photographFor(recipeId).get(InputStream.class);

        assertThat(contentEquals(returnedStream, fileStreamOf(filename)), is(true));

    }

    private InputStream fileStreamOf(final String filename) {
        return this.getClass().getClassLoader().getResourceAsStream(filename);
    }

    private Invocation.Builder photographFor(final String recipeId) {
        return client.target(format("%s%s/photograph", RECIPES_RESOURCE_QUERY_URI, recipeId))
                .request()
                .accept(APPLICATION_OCTET_STREAM_TYPE);
    }

    private void appendFileToTheRecipe(final String recipeId, final String filename) throws IOException {
        final File file = new File(getResource(filename).getFile());


        final HttpEntity httpEntity = MultipartEntityBuilder.create()
                .setMode(BROWSER_COMPATIBLE)
                .addBinaryBody("photoId", file, APPLICATION_OCTET_STREAM, filename)
                .build();

        final HttpPost request = new HttpPost(RECIPES_RESOURCE_URI + recipeId + "/photograph");
        request.setEntity(httpEntity);

        HttpClients.createDefault().execute(request);
    }
}
