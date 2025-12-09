package uk.gov.justice.services.cakeshop.event.listener;

import uk.gov.justice.services.cakeshop.persistence.CakeRepository;
import uk.gov.justice.services.cakeshop.persistence.entity.Cake;
import uk.gov.justice.services.core.annotation.Component;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.messaging.Envelope;

import javax.inject.Inject;

@ServiceComponent(value = Component.EVENT_LISTENER)
public class CakeMadeEventListener {

    @Inject
    CakeRepository cakeRepository;

    @Handles("cakeshop.events.cake-made")
    public void handle(final Envelope<Cake> envelope) {
        //Best practice is to handle a value object rather than an entity
        //because the event typically would not cover an entire entity.
        //But we have not here as this cakeshop is so simple.

        cakeRepository.save(envelope.payload());
    }
}
