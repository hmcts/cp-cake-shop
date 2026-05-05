package uk.gov.justice.services.cakeshop.persistence;

import uk.gov.justice.services.cakeshop.persistence.entity.CakeOrder;

import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

@ApplicationScoped
public class CakeOrderRepository {

    @Inject
    private EntityManager entityManager;

    public CakeOrder save(final CakeOrder cakeOrder) {
        return entityManager.merge(cakeOrder);
    }

    public CakeOrder findBy(final UUID id) {
        return entityManager.find(CakeOrder.class, id);
    }
}
