package uk.gov.justice.services.cakeshop.it.helpers;

import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static jakarta.json.Json.createArrayBuilder;
import static jakarta.json.Json.createObjectBuilder;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;

import uk.gov.justice.services.common.util.Clock;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.messaging.Metadata;

import java.time.ZonedDateTime;
import java.util.UUID;

import jakarta.json.JsonArrayBuilder;


public class CakeshopEventGenerator {

    private final Clock clock = new UtcClock();

    private static final String EVENT_SOURCE = "cakeshop";

    public Event createRecipeAddedEvent(final long seed, final PositionInStreamIterator positionInStreamIterator) {

        //Create events to replay
        final ZonedDateTime now = clock.now();
        final UUID recipeId = randomUUID();

        final long position = positionInStreamIterator.nextPosition();

        final Metadata metadata = metadataBuilder()
                .createdAt(now)
                .withId(randomUUID())
                .withName("cakeshop.events.recipe-added")
                .withCausation(randomUUID(), randomUUID())
                .withStreamId(recipeId)
                .withPosition(position)
                .withSource(EVENT_SOURCE)
                .build();

        final JsonArrayBuilder ingredients = createArrayBuilder();
        
        for (int i = 0; i < ((seed % 3) + 1); i++) {
            ingredients.add(createObjectBuilder()
                    .add("name", "ingredient " + i)
                    .add("quantity", i ));
        }

        final String recipe = createObjectBuilder()
                .add("recipeId", recipeId.toString())
                .add("name", format("Recipe %04d", seed))
                .add("ingredients", ingredients.build())
                .add("glutenFree", false)
                .build()
                .toString();

        return new Event(
                randomUUID(),
                recipeId,
                position,
                "cakeshop.events.recipe-added",
                metadata.asJsonObject().toString(),
                recipe,
                now);
    }

    public Event createRecipeRenamedEvent(final UUID recipeId, final long seed, final int renameNumber, final PositionInStreamIterator positionInStreamIterator) {

        final ZonedDateTime now = clock.now();
        final long position = positionInStreamIterator.nextPosition();
        
        final Metadata metadata = metadataBuilder()
                .createdAt(now)
                .withId(randomUUID())
                .withName("cakeshop.events.recipe-renamed")
                .withCausation(randomUUID(), randomUUID())
                .withStreamId(recipeId)
                .withPosition(position)
                .withSource(EVENT_SOURCE)
                .build();

        final String recipeRename = createObjectBuilder()
                .add("recipeId", recipeId.toString())
                .add("name", format("Recipe %04d (rename  %02d)", seed, renameNumber))
                .build()
                .toString();

        return new Event(
                randomUUID(),
                recipeId,
                position,
                "cakeshop.events.recipe-renamed",
                metadata.asJsonObject().toString(),
                recipeRename,
                now);
    }

    public Event createCakeOrderedEvent(final PositionInStreamIterator positionInStreamIterator) {

        //Create events to replay
        final ZonedDateTime now = clock.now();


        final long position = positionInStreamIterator.nextPosition();
        final UUID orderId = randomUUID();

        final Metadata metadata = metadataBuilder()
                .createdAt(now)
                .withId(randomUUID())
                .withName("cakeshop.events.cake-ordered")
                .withCausation(randomUUID(), randomUUID())
                .withStreamId(orderId)
                .withPosition(position)
                .withSource(EVENT_SOURCE)
                .build();

        final UUID recipeId = randomUUID();

        final String cakeOrdered = createObjectBuilder()
                .add("orderId", orderId.toString())
                .add("recipeId", recipeId.toString())
                .add("deliveryDate", clock.now().toString())
                .build()
                .toString();

        return new Event(
                randomUUID(),
                recipeId,
                position,
                "cakeshop.events.cake-ordered",
                metadata.asJsonObject().toString(),
                cakeOrdered,
                now);
    }


}
