package uk.gov.justice.services.cakeshop.command.api;

import static jakarta.json.Json.createObjectBuilder;
import static uk.gov.justice.services.core.annotation.Component.COMMAND_API;
import static uk.gov.justice.services.core.enveloper.Enveloper.envelop;

import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.sender.Sender;
import uk.gov.justice.services.messaging.Envelope;
import uk.gov.justice.services.messaging.JsonEnvelope;

import jakarta.inject.Inject;
import jakarta.json.JsonObject;

@ServiceComponent(COMMAND_API)
public class MakeCakeCommandApi {

    @Inject
    Sender sender;

    @Handles("cakeshop.make-cake")
    public Envelope<JsonObject> handle(final JsonEnvelope envelope) {
        sender.send(envelop(envelope.payloadAsJsonObject())
                .withName("cakeshop.command.make-cake")
                .withMetadataFrom(envelope));

        return envelop(
                createObjectBuilder()
                        .add("status", "Making Cake")
                        .build())
                .withName("cakeshop.command.make-cake-status")
                .withMetadataFrom(envelope);
    }
}
