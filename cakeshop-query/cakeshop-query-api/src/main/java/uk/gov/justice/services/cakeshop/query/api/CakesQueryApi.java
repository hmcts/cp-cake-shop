package uk.gov.justice.services.cakeshop.query.api;

import static uk.gov.justice.services.core.annotation.Component.QUERY_API;

import uk.gov.justice.services.cakeshop.query.api.request.SearchCake;
import uk.gov.justice.services.cakeshop.query.api.response.CakesView;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.requester.Requester;
import uk.gov.justice.services.messaging.Envelope;

import javax.inject.Inject;

@ServiceComponent(QUERY_API)
public class CakesQueryApi {

    @Inject
    Requester requester;

    @Handles("cakeshop.search-cakes")
    public Envelope<CakesView> cakes(final Envelope<SearchCake> query) {
        return requester.request(query, CakesView.class);
    }

}
