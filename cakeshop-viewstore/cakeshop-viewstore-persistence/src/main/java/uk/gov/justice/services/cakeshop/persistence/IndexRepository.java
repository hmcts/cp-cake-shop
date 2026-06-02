package uk.gov.justice.services.cakeshop.persistence;

import uk.gov.justice.services.cakeshop.persistence.entity.Index;

import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@ApplicationScoped
public class IndexRepository {

    @PersistenceContext(unitName = "Cakeshop")
    private EntityManager entityManager;

    public Index save(final Index index) {
        return entityManager.merge(index);
    }

    public Index findBy(final UUID id) {
        return entityManager.find(Index.class, id);
    }
}
