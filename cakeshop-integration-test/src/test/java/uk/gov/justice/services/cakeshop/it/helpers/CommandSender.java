package uk.gov.justice.services.cakeshop.it.helpers;

import static jakarta.ws.rs.core.Response.Status.ACCEPTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import uk.gov.justice.services.cakeshop.it.params.CakeShopMediaTypes;
import uk.gov.justice.services.cakeshop.it.params.CakeShopUris;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;

public class CommandSender {

    private final Client client;
    private final EventFactory eventFactory;

    public CommandSender(final Client client, final EventFactory eventFactory) {
        this.client = client;
        this.eventFactory = eventFactory;
    }

    public ApiResponse makeCake(final String recipeId, final String cakeId) {
        final Response jaxrRsResponse = client.target(String.format(CakeShopUris.CAKES_RESOURCE_URI_FORMAT, recipeId, cakeId))
                .request()
                .accept(CakeShopMediaTypes.MAKE_CAKE_STATUS_MEDIA_TYPE)
                .post(Entity.entity("{}", CakeShopMediaTypes.MAKE_CAKE_MEDIA_TYPE));
        assertThat(jaxrRsResponse.getStatus(), is(ACCEPTED.getStatusCode()));

        return ApiResponse.from(jaxrRsResponse);
    }

    public void addRecipe(final String recipeId, final String cakeName) {
        client.target(CakeShopUris.RECIPES_RESOURCE_URI + recipeId).request()
                .post(eventFactory.recipeEntity(cakeName, false));
    }
}
