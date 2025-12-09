package uk.gov.justice.services.cakeshop.query.api;

import static uk.gov.justice.services.core.annotation.Component.QUERY_API;

import uk.gov.justice.services.cakeshop.query.api.request.SearchRecipes;
import uk.gov.justice.services.cakeshop.query.api.response.RecipesView;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.requester.Requester;
import uk.gov.justice.services.messaging.Envelope;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.inject.Inject;

@ServiceComponent(QUERY_API)
public class RecipesQueryApi {

    @Inject
    Requester requester;

    @Handles("cakeshop.search-recipes")
    public JsonEnvelope searchRecipes(final JsonEnvelope query) {
        return requester.request(query);
    }

    @Handles("cakeshop.get-recipe")
    public JsonEnvelope getRecipe(final JsonEnvelope query) {
        return requester.request(query);
    }

    @Handles("cakeshop.query-recipes")
    public Envelope<RecipesView> queryRecipes(final Envelope<SearchRecipes> query) {
        return requester.request(query, RecipesView.class);
    }

    @Handles("cakeshop.get-recipe-photograph")
    public JsonEnvelope getRecipePhotograph(final JsonEnvelope query) {
        return requester.request(query);
    }
}
