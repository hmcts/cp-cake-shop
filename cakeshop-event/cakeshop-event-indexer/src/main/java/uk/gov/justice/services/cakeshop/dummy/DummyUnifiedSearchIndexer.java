package uk.gov.justice.services.cakeshop.dummy;

import static java.util.UUID.fromString;

import uk.gov.justice.services.cakeshop.persistence.IndexRepository;
import uk.gov.justice.services.cakeshop.persistence.entity.Index;
import uk.gov.justice.services.messaging.Envelope;
import uk.gov.justice.services.unifiedsearch.UnifiedSearchIndexer;

import java.time.ZonedDateTime;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.JsonObject;

@ApplicationScoped
public class DummyUnifiedSearchIndexer implements UnifiedSearchIndexer {

    @Inject
    private IndexRepository indexRepository;

    @Override
    public void indexData(final Envelope<JsonObject> eventWithJoltTransformedPayload) {
        final JsonObject payload = eventWithJoltTransformedPayload.payload();
        final UUID recipeId = fromString(payload.getString("recipeId"));
        final ZonedDateTime deliveryDate = ZonedDateTime.parse(payload.getString("deliveryDate"));
        final Index index = new Index(recipeId, deliveryDate);
        indexRepository.save(index);
    }
}
