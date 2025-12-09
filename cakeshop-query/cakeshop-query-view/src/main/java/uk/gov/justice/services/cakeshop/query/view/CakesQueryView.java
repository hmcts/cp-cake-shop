package uk.gov.justice.services.cakeshop.query.view;

import static uk.gov.justice.services.core.annotation.Component.QUERY_VIEW;
import static uk.gov.justice.services.core.enveloper.Enveloper.envelop;

import uk.gov.justice.services.cakeshop.query.view.response.CakesView;
import uk.gov.justice.services.cakeshop.query.view.service.CakeService;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.messaging.Envelope;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.inject.Inject;


@ServiceComponent(QUERY_VIEW)
public class CakesQueryView {

    @Inject
    CakeService service;

    @Handles("cakeshop.search-cakes")
    public Envelope<CakesView> cakes(final JsonEnvelope query) {
        return envelop(service.cakes())
                .withName("cakeshop.search-cakes")
                .withMetadataFrom(query);
    }
}
