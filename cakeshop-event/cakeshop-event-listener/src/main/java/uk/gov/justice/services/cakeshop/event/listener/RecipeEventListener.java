package uk.gov.justice.services.cakeshop.event.listener;

import static java.util.UUID.randomUUID;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;

import uk.gov.justice.services.cakeshop.domain.event.RecipeAdded;
import uk.gov.justice.services.cakeshop.event.listener.converter.RecipeAddedToIngredientsConverter;
import uk.gov.justice.services.cakeshop.event.listener.converter.RecipeAddedToRecipeConverter;
import uk.gov.justice.services.cakeshop.persistence.IngredientRepository;
import uk.gov.justice.services.cakeshop.persistence.RecipeRepository;
import uk.gov.justice.services.cakeshop.persistence.entity.Ingredient;
import uk.gov.justice.services.cakeshop.persistence.entity.Recipe;
import uk.gov.justice.services.common.converter.JsonObjectToObjectConverter;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;

import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ServiceComponent(EVENT_LISTENER)
public class RecipeEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecipeEventListener.class);
    private static final String FIELD_RECIPE_ID = "recipeId";
    private static final String FIELD_PHOTO_ID = "photoId";

    @Inject
    JsonObjectToObjectConverter jsonObjectConverter;

    @Inject
    RecipeAddedToRecipeConverter recipeAddedToRecipeConverter;

    @Inject
    RecipeAddedToIngredientsConverter recipeAddedToIngredientsConverter;

    @Inject
    RecipeRepository recipeRepository;

    @Inject
    IngredientRepository ingredientRepository;

    @Handles("cakeshop.events.recipe-added")
    public void recipeAdded(final JsonEnvelope event) {

        final String recipeId = event.payloadAsJsonObject().getString(FIELD_RECIPE_ID);
        LOGGER.trace("=============> Inside add-recipe Event Listener. RecipeId: " + recipeId);

        final RecipeAdded recipeAdded = jsonObjectConverter.convert(event.payloadAsJsonObject(), RecipeAdded.class);

        final Recipe recipe = recipeAddedToRecipeConverter.convert(recipeAdded);

        if ("DELIBERATELY_FAIL".equals(recipe.getName())) {
            final Recipe malformedRecipe = new Recipe(
                    randomUUID(),
                    null,
                    true,
                    randomUUID());

            // should throw exception due to null recipe name
            recipeRepository.save(malformedRecipe);
        } else {
            recipeRepository.save(recipe);
        }

        LOGGER.trace("=====================================================> Recipe saved, RecipeId: " + recipeId);

        for (final Ingredient ingredient : recipeAddedToIngredientsConverter.convert(recipeAdded)) {
            if (ingredientRepository.findByNameIgnoreCase(ingredient.getName()).isEmpty()) {
                LOGGER.trace("=============> Inside add-recipe Event Listener about to save Ingredient Id: " + ingredient.getId());
                ingredientRepository.save(ingredient);
                LOGGER.trace("=====================================================> Ingredient saved, Ingredient Id: " + ingredient.getId());
            } else {
                LOGGER.trace("=====================================================> Skipped adding ingredient as it already exists, Ingredient Name: " + ingredient.getName());
            }
        }
    }

    @Handles("cakeshop.events.recipe-renamed")
    public void recipeRenamed(final JsonEnvelope event) {

        final String recipeId = event.payloadAsJsonObject().getString(FIELD_RECIPE_ID);
        final String recipeName = event.payloadAsJsonObject().getString("name");
        LOGGER.trace("=============> Inside rename-recipe Event Listener. RecipeId: " + recipeId);

        final Recipe recipe = recipeRepository.findBy(UUID.fromString(recipeId));
        recipe.setName(recipeName);
        recipeRepository.save(recipe);
    }

    @Handles("cakeshop.events.recipe-removed")
    public void recipeRemoved(final JsonEnvelope event) {
        final String recipeId = event.payloadAsJsonObject().getString(FIELD_RECIPE_ID);
        LOGGER.trace("=============> Inside remove-recipe Event Listener about to find recipeId: " + recipeId);
        final Recipe recipeFound = recipeRepository.findBy(UUID.fromString(recipeId));
        LOGGER.trace("=============> Found remove-recipe Event Listener. RecipeId: " + recipeFound);
        recipeRepository.remove(recipeFound);
    }

    @Handles("cakeshop.events.recipe-photograph-added")
    public void recipePhotographAdded(final JsonEnvelope event) {

        final String recipeId = event.payloadAsJsonObject().getString(FIELD_RECIPE_ID);
        final String photoId = event.payloadAsJsonObject().getString(FIELD_PHOTO_ID);
        LOGGER.trace("=============> Inside recipe-photograph-added Event Listener. RecipeId: " + recipeId);

        final Recipe recipe = recipeRepository.findBy(UUID.fromString(recipeId));
        recipe.setPhotoId(UUID.fromString(photoId));
        recipeRepository.save(recipe);
    }

}
