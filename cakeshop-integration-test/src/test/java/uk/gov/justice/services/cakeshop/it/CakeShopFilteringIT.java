package uk.gov.justice.services.cakeshop.it;

import static com.jayway.jsonassert.JsonAssert.emptyCollection;
import static com.jayway.jsonassert.JsonAssert.with;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static jakarta.ws.rs.core.Response.Status.OK;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_URI;

import uk.gov.justice.services.cakeshop.it.helpers.ApiResponse;
import uk.gov.justice.services.cakeshop.it.helpers.CommandSender;
import uk.gov.justice.services.cakeshop.it.helpers.EventFactory;
import uk.gov.justice.services.cakeshop.it.helpers.Querier;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;

import jakarta.ws.rs.client.Client;

import org.apache.http.message.BasicNameValuePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

        await().until(() -> querier.recipesQueryResult().body().contains(recipeId));

        final ApiResponse response = querier.recipesQueryResult(singletonList(new BasicNameValuePair("pagesize", "1")));
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

        await().until(() -> querier.recipesQueryResult().body().contains(recipeId2));

        final ApiResponse response = querier.recipesQueryResult(asList(
                new BasicNameValuePair("pagesize", "30"),
                new BasicNameValuePair("glutenFree", "true")));

        assertThat(response.httpCode(), is(OK.getStatusCode()));

        with(response.body())
                .assertThat("$.recipes[?(@.id=='" + recipeId + "')]", emptyCollection())
                .assertThat("$.recipes[?(@.id=='" + recipeId2 + "')].name", hasItem("Oat cake"));
    }
}
