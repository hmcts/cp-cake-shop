package uk.gov.justice.services.cakeshop.command.api;

import static uk.gov.justice.services.core.annotation.Component.COMMAND_API;
import static uk.gov.justice.services.core.enveloper.Enveloper.envelop;

import uk.gov.justice.services.core.annotation.FeatureControl;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.sender.Sender;
import uk.gov.justice.services.messaging.JsonEnvelope;

import jakarta.inject.Inject;

import org.slf4j.Logger;

@ServiceComponent(COMMAND_API)
public class RecipeCommandApi {

    @Inject
    Sender sender;

    @Inject
    Logger logger;

    @Handles("cakeshop.add-recipe")
    public void addRecipe(final JsonEnvelope envelope) {
        sender.send(
                envelop(envelope.payloadAsJsonObject())
                        .withName("cakeshop.command.add-recipe")
                        .withMetadataFrom(envelope));
    }

    @Handles("cakeshop.add-recipe-v2")
    @FeatureControl("recipes-have-allergens-specified")
    public void addRecipeWithAllergenSupport(final JsonEnvelope envelope) {
        logger.warn("Call to in progress method. Feature 'recipes-have-allergens-specified' is enabled");
    }

    @Handles("cakeshop.rename-recipe")
    public void renameRecipe(final JsonEnvelope envelope) {
        sender.send(
                envelop(envelope.payloadAsJsonObject())
                        .withName("cakeshop.command.rename-recipe")
                        .withMetadataFrom(envelope));
    }

    @Handles("cakeshop.remove-recipe")
    public void removeRecipe(final JsonEnvelope envelope) {
        sender.send(
                envelop(envelope.payloadAsJsonObject())
                        .withName("cakeshop.command.remove-recipe")
                        .withMetadataFrom(envelope));
    }

    @Handles("cakeshop.upload-photograph")
    public void uploadPhotograph(final JsonEnvelope envelope) {
        sender.send(
                envelop(envelope.payloadAsJsonObject())
                        .withName("cakeshop.command.upload-photograph")
                        .withMetadataFrom(envelope));
    }
}
