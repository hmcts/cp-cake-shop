package uk.gov.justice.services.cakeshop.it.helpers;

import static jakarta.json.Json.createArrayBuilder;
import static jakarta.json.Json.createObjectBuilder;
import static jakarta.ws.rs.client.Entity.entity;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopMediaTypes.ADD_RECIPE_MEDIA_TYPE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopMediaTypes.RENAME_RECIPE_MEDIA_TYPE;

import jakarta.ws.rs.client.Entity;

public class EventFactory {

    public Entity<String> renameRecipeEntity(final String recipeName) {
        return entity(
                createObjectBuilder()
                        .add("name", recipeName)
                        .build().toString(),
                RENAME_RECIPE_MEDIA_TYPE);
    }

    public Entity<String> recipeEntity(String recipeName, boolean glutenFree) {
        return entity(
                createObjectBuilder()
                        .add("name", recipeName)
                        .add("glutenFree", glutenFree)
                        .add("ingredients", createArrayBuilder()
                                .add(createObjectBuilder()
                                        .add("name", "someIngredient")
                                        .add("quantity", 1)
                                ).build()
                        ).build().toString(),
                ADD_RECIPE_MEDIA_TYPE);
    }

    public Entity<String> recipeEntity(final String recipeName) {
        final boolean glutenFree = false;
        return recipeEntity(recipeName, glutenFree);
    }
}
