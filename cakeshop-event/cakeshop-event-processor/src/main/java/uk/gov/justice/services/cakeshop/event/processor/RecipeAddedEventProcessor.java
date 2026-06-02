package uk.gov.justice.services.cakeshop.event.processor;

import static uk.gov.justice.services.core.annotation.Component.EVENT_PROCESSOR;

import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.sender.Sender;
import uk.gov.justice.services.messaging.JsonEnvelope;

import jakarta.inject.Inject;

@ServiceComponent(EVENT_PROCESSOR)
public class RecipeAddedEventProcessor {

    @Inject
    Sender sender;

    @Handles("cakeshop.events.recipe-added")
    public void recipeAdded(final JsonEnvelope event) {
        sender.send(event);
    }
}
