package uk.gov.justice.services.cakeshop.persistence;

import static java.text.MessageFormat.format;

import uk.gov.justice.services.cakeshop.persistence.entity.Recipe;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.TypedQuery;

@ApplicationScoped
public class RecipeRepository {

    @PersistenceContext(unitName = "Cakeshop")
    private EntityManager entityManager;

    public Recipe save(final Recipe recipe) {
        return entityManager.merge(recipe);
    }

    public Recipe findBy(final UUID id) {
        return entityManager.find(Recipe.class, id);
    }

    public void remove(final Recipe recipe) {
        entityManager.remove(entityManager.contains(recipe) ? recipe : entityManager.merge(recipe));
    }

    public List<Recipe> findAll() {
        return entityManager.createQuery("SELECT r FROM Recipe r", Recipe.class).getResultList();
    }

    public List<Recipe> findBy(final int pageSize, final Optional<String> name, final Optional<Boolean> glutenFree) {
        final StringBuilder jpql = new StringBuilder("SELECT r FROM Recipe r WHERE 1=1");
        if (name.isPresent()) {
            jpql.append(" AND r.name LIKE :name");
        }
        if (glutenFree.isPresent()) {
            jpql.append(" AND r.glutenFree = :glutenFree");
        }
        jpql.append(" ORDER BY r.name");

        final TypedQuery<Recipe> query = entityManager.createQuery(jpql.toString(), Recipe.class);
        if (name.isPresent()) {
            query.setParameter("name", format("%{0}%", name.get()));
        }
        if (glutenFree.isPresent()) {
            query.setParameter("glutenFree", glutenFree.get());
        }
        return query.setMaxResults(pageSize).getResultList();
    }
}
