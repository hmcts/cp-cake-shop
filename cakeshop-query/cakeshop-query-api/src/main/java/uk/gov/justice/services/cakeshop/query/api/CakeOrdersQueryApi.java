package uk.gov.justice.services.cakeshop.query.api;


import static uk.gov.justice.services.core.annotation.Component.QUERY_API;

import uk.gov.justice.services.cakeshop.query.api.request.SearchCakeOrder;
import uk.gov.justice.services.cakeshop.query.api.response.CakeOrderView;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.requester.Requester;
import uk.gov.justice.services.messaging.Envelope;

import javax.inject.Inject;

@ServiceComponent(QUERY_API)
public class CakeOrdersQueryApi {

    @Inject
    Requester requester;

    @Handles("cakeshop.get-order")
    public Envelope<CakeOrderView> getOrder(final Envelope<SearchCakeOrder> query) {
        return requester.request(query, CakeOrderView.class);
    }
}
