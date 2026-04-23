package uk.gov.justice.services.cakeshop.persistence;

import uk.gov.justice.services.cakeshop.persistence.entity.Index;

import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

@ApplicationScoped
public class IndexRepository {

    @Inject
    private EntityManager entityManager;

    public Index save(final Index index) {
        return entityManager.merge(index);
    }

    public Index findBy(final UUID id) {
        return entityManager.find(Index.class, id);
    }
}
