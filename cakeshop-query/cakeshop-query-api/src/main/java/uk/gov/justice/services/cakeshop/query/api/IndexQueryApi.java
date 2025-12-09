package uk.gov.justice.services.cakeshop.query.api;

import static uk.gov.justice.services.core.annotation.Component.QUERY_API;

import uk.gov.justice.services.cakeshop.query.api.request.SearchIndex;
import uk.gov.justice.services.cakeshop.query.api.response.IndexView;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.requester.Requester;
import uk.gov.justice.services.messaging.Envelope;

import javax.inject.Inject;

@ServiceComponent(QUERY_API)
public class IndexQueryApi {

    @Inject
    private Requester requester;

    @Handles("cakeshop.get-index")
    public Envelope<IndexView> getIndex(final Envelope<SearchIndex> query) {
        return requester.request(query, IndexView.class);
    }
}
