package uk.gov.justice.services.cakeshop.persistence;

import uk.gov.justice.services.cakeshop.persistence.entity.Ingredient;

import java.util.List;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@ApplicationScoped
public class IngredientRepository {

    @PersistenceContext(unitName = "Cakeshop")
    private EntityManager entityManager;

    public Ingredient save(final Ingredient ingredient) {
        return entityManager.merge(ingredient);
    }

    public Ingredient findBy(final UUID id) {
        return entityManager.find(Ingredient.class, id);
    }

    public List<Ingredient> findAll() {
        return entityManager.createQuery("SELECT i FROM Ingredient i", Ingredient.class).getResultList();
    }

    /**
     * Find all {@link Ingredient} by ingedientName (case-insensitive). Accepts '%' wildcard
     * values.
     *
     * @param ingedientName to retrieve the ingredient by, including wildcard characters.
     * @return List of matching ingredients. Never returns null.
     */
    public List<Ingredient> findByNameIgnoreCase(final String ingredientName) {
        return entityManager.createQuery(
                "SELECT i FROM Ingredient i WHERE LOWER(i.name) LIKE LOWER(:name)", Ingredient.class)
                .setParameter("name", ingredientName)
                .getResultList();
    }
}
