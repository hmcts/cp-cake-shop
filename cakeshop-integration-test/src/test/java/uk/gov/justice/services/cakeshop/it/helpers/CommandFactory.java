package uk.gov.justice.services.cakeshop.it.helpers;

import static jakarta.json.Json.createArrayBuilder;
import static jakarta.json.Json.createObjectBuilder;

public class CommandFactory {

    public String addRecipeCommand() {
        return createObjectBuilder()
                .add("name", "Chocolate muffin in six easy steps")
                .add("glutenFree", false)
                .add("ingredients", createArrayBuilder()
                        .add(createObjectBuilder()
                                .add("name", "chocolate")
                                .add("quantity", 1)
                        ).build())
                .build().toString();
    }

    public String addRecipeCommandWithAllergensListed() {
        return createObjectBuilder()
                .add("name", "Chocolate muffin in six easy steps")
                .add("glutenFree", false)
                .add("ingredients", createArrayBuilder()
                        .add(createObjectBuilder()
                                .add("name", "chocolate")
                                .add("quantity", 1)
                                .add("allergen", false))
                        .add(createObjectBuilder()
                                .add("name", "eggs")
                                .add("quantity", 2)
                                .add("allergen", true))
                        .build())
                .build().toString();
    }

    public String addRecipeCommandByName(final String recipeName) {
        return createObjectBuilder()
                .add("name", recipeName)
                .add("glutenFree", false)
                .add("ingredients", createArrayBuilder()
                        .add(createObjectBuilder()
                                .add("name", "ingredient")
                                .add("quantity", 1)
                        ).build())
                .build().toString();
    }
}
