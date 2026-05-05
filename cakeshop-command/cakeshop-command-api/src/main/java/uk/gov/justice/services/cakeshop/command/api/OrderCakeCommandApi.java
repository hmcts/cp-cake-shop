package uk.gov.justice.services.cakeshop.command.api;

import static uk.gov.justice.services.core.annotation.Component.COMMAND_API;
import static uk.gov.justice.services.core.enveloper.Enveloper.envelop;

import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.sender.Sender;
import uk.gov.justice.services.messaging.JsonEnvelope;

import jakarta.inject.Inject;

@ServiceComponent(COMMAND_API)
public class OrderCakeCommandApi {

    @Inject
    Sender sender;

    @Handles("cakeshop.order-cake")
    public void orderCake(final JsonEnvelope envelope) {
        sender.send(
                envelop(envelope.payloadAsJsonObject())
                        .withName("cakeshop.command.order-cake")
                        .withMetadataFrom(envelope));
    }
}
