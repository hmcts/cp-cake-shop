package uk.gov.justice.services.cakeshop.dummy;

import uk.gov.justice.services.unifiedsearch.UnifiedSearchIndexer;
import uk.gov.justice.services.unifiedsearch.UnifiedSearchName;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Inject;

@ApplicationScoped
public class DummyUnifiedSearchIndexerProducer {

    @Inject
    private DummyUnifiedSearchIndexer dummyUnifiedSearchIndexer;

    @Produces
    @UnifiedSearchName
    public UnifiedSearchIndexer unifiedSearchClient(final InjectionPoint injectionPoint) {
        return dummyUnifiedSearchIndexer;
    }
}
