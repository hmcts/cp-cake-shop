package uk.gov.justice.services.cakeshop.it;

import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.client.Client;
import org.apache.http.message.BasicNameValuePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.justice.services.cakeshop.it.helpers.ApiResponse;
import uk.gov.justice.services.cakeshop.it.helpers.CommandSender;
import uk.gov.justice.services.cakeshop.it.helpers.EventFactory;
import uk.gov.justice.services.cakeshop.it.helpers.Querier;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;

import static com.jayway.jsonassert.JsonAssert.emptyCollection;
import static com.jayway.jsonassert.JsonAssert.with;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.Response.Status.OK;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_URI;

import org.junit.jupiter.api.extension.ExtendWith;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseResetExtension;

@ExtendWith(DatabaseResetExtension.class)
public class CakeShopFilteringIT {

    private final EventFactory eventFactory = new EventFactory();

    private Client client;
    private Querier querier;
    private CommandSender commandSender;

    @BeforeEach
    public void before() throws Exception {
        client = new RestEasyClientFactory().createResteasyClient();
        querier = new Querier(client);
        commandSender = new CommandSender(client, eventFactory);
    }

    @AfterEach
    public void cleanup() throws Exception {
        client.close();
    }

    @Test
    public void shouldFilterRecipesUsingPageSize() {
        //adding 2 recipes
        final String recipeId = randomUUID().toString();
        commandSender.addRecipe(recipeId, "Absolutely cheesy cheese cake");

        final String recipeId2 = randomUUID().toString();
        commandSender.addRecipe(recipeId2, "Chocolate muffin");

        final AtomicReference<ApiResponse> responseRef = new AtomicReference<>();
        await().atMost(ofSeconds(30)).until(() -> {
            final ApiResponse response = querier.recipesQueryResult(singletonList(new BasicNameValuePair("pagesize", "1")));
            responseRef.set(response);
            return response.body().contains(recipeId);
        });

        final ApiResponse response = responseRef.get();
        assertThat(response.httpCode(), is(OK.getStatusCode()));

        with(response.body())
                .assertThat("$.recipes[?(@.id=='" + recipeId2 + "')]", emptyCollection())
                .assertThat("$.recipes[?(@.id=='" + recipeId + "')].name", hasItem("Absolutely cheesy cheese cake"));
    }

    @Test
    public void shouldFilterGlutenFreeRecipes() {
        //adding 2 recipes
        final String recipeId = randomUUID().toString();
        client.target(RECIPES_RESOURCE_URI + recipeId).request()
                .post(eventFactory.recipeEntity("Muffin", false));

        final String recipeId2 = randomUUID().toString();
        client.target(RECIPES_RESOURCE_URI + recipeId2).request()
                .post(eventFactory.recipeEntity("Oat cake", true));

        final AtomicReference<ApiResponse> responseRef = new AtomicReference<>();
        await().atMost(ofSeconds(30)).until(() -> {
            final ApiResponse response = querier.recipesQueryResult(asList(
                    new BasicNameValuePair("pagesize", "1"),
                    new BasicNameValuePair("glutenFree", "true")));
            responseRef.set(response);
            return response.body().contains(recipeId2);
        });

        final ApiResponse response = responseRef.get();
        assertThat(response.httpCode(), is(OK.getStatusCode()));

        with(response.body())
                .assertThat("$.recipes[?(@.id=='" + recipeId + "')]", emptyCollection())
                .assertThat("$.recipes[?(@.id=='" + recipeId2 + "')].name", hasItem("Oat cake"));
    }
}
