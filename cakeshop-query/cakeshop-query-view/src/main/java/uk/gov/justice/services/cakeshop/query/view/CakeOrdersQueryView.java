package uk.gov.justice.services.cakeshop.query.view;

import static uk.gov.justice.services.core.enveloper.Enveloper.envelop;

import uk.gov.justice.services.cakeshop.query.view.request.SearchCakeOrder;
import uk.gov.justice.services.cakeshop.query.view.response.CakeOrderView;
import uk.gov.justice.services.cakeshop.query.view.service.CakeOrderService;
import uk.gov.justice.services.core.annotation.Component;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.messaging.Envelope;

import javax.inject.Inject;

@ServiceComponent(Component.QUERY_VIEW)
public class CakeOrdersQueryView {

    @Inject
    CakeOrderService service;

    @Handles("cakeshop.get-order")
    public Envelope<CakeOrderView> findOrder(final Envelope<SearchCakeOrder> query) {
        final String orderId = query.payload().getOrderId().toString();

        return envelop(service.findOrder(orderId))
                .withName("cakeshop.get-order")
                .withMetadataFrom(query);
    }
}
