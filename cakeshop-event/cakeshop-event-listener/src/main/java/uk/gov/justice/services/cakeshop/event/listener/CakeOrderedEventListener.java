package uk.gov.justice.services.cakeshop.event.listener;

import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;

import uk.gov.justice.services.cakeshop.persistence.CakeOrderRepository;
import uk.gov.justice.services.cakeshop.persistence.entity.CakeOrder;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.messaging.Envelope;

import javax.inject.Inject;

@ServiceComponent(EVENT_LISTENER)
public class CakeOrderedEventListener {

    @Inject
    CakeOrderRepository repository;

    @Handles("cakeshop.events.cake-ordered")
    public void handle(final Envelope<CakeOrder> envelope) {
        //Best practice is to handle a value object rather than an entity
        //because the event typically would not cover an entire entity.
        //But we have not here as this cakeshop is so simple.
        repository.save(envelope.payload());
    }
}
