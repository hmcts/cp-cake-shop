package uk.gov.justice.services.cakeshop.persistence;

import uk.gov.justice.services.cakeshop.persistence.entity.Cake;

import java.util.List;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@ApplicationScoped
public class CakeRepository {

    @PersistenceContext(unitName = "Cakeshop")
    private EntityManager entityManager;

    public Cake save(final Cake cake) {
        return entityManager.merge(cake);
    }

    public Cake findBy(final UUID id) {
        return entityManager.find(Cake.class, id);
    }

    public List<Cake> findAll() {
        return entityManager.createQuery("SELECT c FROM Cake c", Cake.class).getResultList();
    }
}
