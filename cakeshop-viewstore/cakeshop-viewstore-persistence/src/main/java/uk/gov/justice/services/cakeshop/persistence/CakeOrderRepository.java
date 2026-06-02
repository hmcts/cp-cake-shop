package uk.gov.justice.services.cakeshop.persistence;

import uk.gov.justice.services.cakeshop.persistence.entity.CakeOrder;

import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@ApplicationScoped
public class CakeOrderRepository {

    @PersistenceContext(unitName = "Cakeshop")
    private EntityManager entityManager;

    public CakeOrder save(final CakeOrder cakeOrder) {
        return entityManager.merge(cakeOrder);
    }

    public CakeOrder findBy(final UUID id) {
        return entityManager.find(CakeOrder.class, id);
    }
}
