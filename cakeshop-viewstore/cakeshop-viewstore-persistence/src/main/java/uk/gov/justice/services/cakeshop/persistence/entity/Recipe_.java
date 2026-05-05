package uk.gov.justice.services.cakeshop.persistence.entity;

import java.util.UUID;
import jakarta.persistence.metamodel.SingularAttribute;
import jakarta.persistence.metamodel.StaticMetamodel;

@StaticMetamodel(Recipe.class)
public abstract class Recipe_ {
    public static volatile SingularAttribute<Recipe, UUID> id;
    public static volatile SingularAttribute<Recipe, String> name;
    public static volatile SingularAttribute<Recipe, Boolean> glutenFree;
    public static volatile SingularAttribute<Recipe, UUID> photoId;
}
