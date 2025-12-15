package uk.gov.justice.services.cakeshop.custom.api;

import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static uk.gov.justice.services.core.enveloper.Enveloper.envelop;

import uk.gov.justice.services.cakeshop.custom.api.response.OvenStatus;
import uk.gov.justice.services.cakeshop.custom.api.response.OvensStatus;
import uk.gov.justice.services.core.annotation.CustomServiceComponent;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.messaging.Envelope;
import uk.gov.justice.services.messaging.JsonEnvelope;

@CustomServiceComponent("CUSTOM_API")
public class OvenCustomApi {

    @Handles("cakeshop.ovens-status")
    public Envelope<OvensStatus> status(final JsonEnvelope query) {

        final OvensStatus status = new OvensStatus(asList(
                new OvenStatus(randomUUID(), "Big Oven", 250, true),
                new OvenStatus(randomUUID(), "Large Oven", 0, false)));

        return envelop(status)
                .withName("cakeshop.ovens-status")
                .withMetadataFrom(query);
    }
}
