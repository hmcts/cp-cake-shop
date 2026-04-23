package uk.gov.justice.services.cakeshop.query.view.service;

import uk.gov.justice.services.cakeshop.persistence.IndexRepository;
import uk.gov.justice.services.cakeshop.persistence.entity.Index;
import uk.gov.justice.services.cakeshop.query.view.response.IndexView;

import java.util.UUID;

import jakarta.inject.Inject;

public class IndexService {

    @Inject
    private IndexRepository indexRepository;

    public IndexView findIndexBy(final String indexId) {
        final Index index = indexRepository.findBy(UUID.fromString(indexId));
        return index != null ? new IndexView(index.getIndexId(), index.getDeliveryDate()) : null;
    }
}

