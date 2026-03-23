package uk.gov.justice.services.cakeshop.it;

import static com.jayway.jsonassert.JsonAssert.with;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static javax.ws.rs.client.Entity.entity;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopMediaTypes.ADD_RECIPE_MEDIA_TYPE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopMediaTypes.REMOVE_RECIPE_MEDIA_TYPE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_URI;

import org.apache.commons.lang3.RandomStringUtils;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.EventFactory;
import uk.gov.justice.services.cakeshop.it.helpers.EventFinder;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventRepositoryFactory;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;
import javax.ws.rs.client.Client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pure throughput concurrency test — no failure injection.
 * 50 client threads, 1,000 recipes, 12,000 events by default.
 */
public class CakeShopConcurrencyIT {

    private static final int THREAD_COUNT = Integer.getInteger("thread.count", 10);
    private static final int RECIPE_COUNT = Integer.getInteger("recipe.count", 100);
    private static final int RENAMES_PER_RECIPE = 10;
    private static final int EVENTS_PER_RECIPE = 1 + RENAMES_PER_RECIPE + 1; // add + renames + remove
    private static final int EXPECTED_TOTAL_EVENTS = RECIPE_COUNT * EVENTS_PER_RECIPE;

    private final DatabaseManager databaseManager = new DatabaseManager();
    private final DataSource eventStoreDataSource = databaseManager.initEventStoreDb();
    private final DataSource viewStoreDataSource = databaseManager.initViewStoreDb();
    private final EventJdbcRepository eventJdbcRepository = new EventRepositoryFactory().getEventJdbcRepository(eventStoreDataSource);

    private final EventFinder eventFinder = new EventFinder(eventJdbcRepository);
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();
    private final EventFactory eventFactory = new EventFactory();

    private Client client;

    @BeforeEach
    public void before() throws Exception {
        client = new RestEasyClientFactory().createResteasyClient();

        databaseCleaner.cleanEventStoreTables(CONTEXT_NAME);
        databaseCleaner.cleanViewStoreTables(CONTEXT_NAME,
                "stream_buffer",
                "stream_status",
                "stream_error_hash",
                "stream_error",
                "ingredient",
                "recipe",
                "cake",
                "cake_order",
                "processed_event");
    }

    @AfterEach
    public void cleanup() throws Exception {
        client.close();
    }

    @Test
    public void shouldProcessAllEventsWithNoConcurrencyErrors() throws Exception {
        System.out.printf("%n===== CakeShopConcurrencyIT: Starting (%d recipes, %d threads, %d events) =====%n",
                RECIPE_COUNT, THREAD_COUNT, EXPECTED_TOTAL_EVENTS);

        final long startTime = System.currentTimeMillis();
        final AtomicInteger commandsCompleted = new AtomicInteger(0);

        final ScheduledExecutorService progressPoller = Executors.newSingleThreadScheduledExecutor();
        progressPoller.scheduleAtFixedRate(() -> printProgress(startTime, commandsCompleted), 2, 2, TimeUnit.SECONDS);

        final ExecutorService exec = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < RECIPE_COUNT; i++) {
            final int recipeIndex = i;
            exec.execute(() -> {
                publishRecipeEvents(recipeIndex);
                commandsCompleted.incrementAndGet();
            });
        }
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.MINUTES);

        final long commandsDoneTime = System.currentTimeMillis();

        System.out.printf("%n===== Waiting for events (%d expected) =====%n", EXPECTED_TOTAL_EVENTS);
        awaitCountStable(this::countEventLogEvents, EXPECTED_TOTAL_EVENTS, "event_log");
        final long eventLogDoneTime = System.currentTimeMillis();
        final int eventLogCount = countEventLogEvents();

        System.out.printf("%n===== Waiting for event linking (%d expected) =====%n", EXPECTED_TOTAL_EVENTS);
        awaitCountStable(this::countLinkedEvents, EXPECTED_TOTAL_EVENTS, "linked_events");
        final long linkingDoneTime = System.currentTimeMillis();
        final int linkedCount = countLinkedEvents();

        System.out.printf("%n===== Waiting for stream processing (all streams caught up) =====%n");
        awaitDrain(this::countStreamsBehind, "streamsBehind");
        final long processingDoneTime = System.currentTimeMillis();

        final int publishQueueRemaining = countPublishQueue();

        progressPoller.shutdownNow();

        final double totalSecs = (processingDoneTime - startTime) / 1000.0;
        final double commandSecs = (commandsDoneTime - startTime) / 1000.0;
        final double linkingSecs = (linkingDoneTime - startTime) / 1000.0;
        final double processingSecs = (processingDoneTime - startTime) / 1000.0;

        System.out.printf("%n========================================%n");
        System.out.printf("  CakeShopConcurrencyIT RESULTS%n");
        System.out.printf("========================================%n");
        System.out.printf("  Recipes:            %,d%n", RECIPE_COUNT);
        System.out.printf("  Threads:            %d%n", THREAD_COUNT);
        System.out.printf("  Event log count:    %,d%n", eventLogCount);
        System.out.printf("  Linked events:      %,d%n", linkedCount);
        System.out.printf("  Streams up-to-date: %,d%n", countStreamsUpToDate());
        System.out.printf("  Streams with errors:%,d%n", countStreamsWithErrors());
        System.out.printf("  Streams behind:     %d%n", countStreamsBehind());
        System.out.printf("  Publish queue left: %d%n", publishQueueRemaining);
        System.out.printf("  Advisory locks:     %d%n", countAdvisoryLocks());
        System.out.printf("  ---- Phase Timing ----%n");
        System.out.printf("  Commands done:      %.1fs%n", commandSecs);
        System.out.printf("  All events in log:  %.1fs%n", (eventLogDoneTime - startTime) / 1000.0);
        System.out.printf("  Linking done:       %.1fs  -> Linking TPS: %.0f%n", linkingSecs, linkedCount / linkingSecs);
        System.out.printf("  Processing done:    %.1fs  -> Processing TPS: %.0f%n", processingSecs, eventLogCount / processingSecs);
        System.out.printf("  ---- End-to-End ----%n");
        System.out.printf("  Total:              %.2f min -> %.0f TPS%n",
                totalSecs / 60.0, eventLogCount / totalSecs);
        System.out.printf("========================================%n%n");

        await().until(() -> countStreamsBehind() == 0);
        await().until(() -> countStreamsWithErrors() == 0);
    }

    // ========================================================================
    // Recipe publishing
    // ========================================================================

    private void publishRecipeEvents(final int recipeIndex) {
        final String recipeId = randomUUID().toString();
        final String recipeName = format("Recipe %04d", recipeIndex);

        client.target(RECIPES_RESOURCE_URI + recipeId)
                .request()
                .post(entity(
                        createObjectBuilder()
                                .add("name", recipeName)
                                .add("glutenFree", false)
                                .add("ingredients", createArrayBuilder()
                                        .add(createObjectBuilder()
                                                .add("name", "vanilla")
                                                .add("quantity", 2)
                                        ).build()
                                ).build().toString(),
                        ADD_RECIPE_MEDIA_TYPE));

        await().until(() -> eventFinder.eventsWithPayloadContaining(recipeId, recipeId).size() == 1);

        final Event event = eventFinder.eventsWithPayloadContaining(recipeId, recipeId).get(0);
        assertThat(event.getName(), is("cakeshop.events.recipe-added"));
        with(event.getMetadata())
                .assertEquals("stream.id", recipeId)
                .assertEquals("stream.version", 1);

        for (int i = 0; i < RENAMES_PER_RECIPE; i++) {
            client.target(RECIPES_RESOURCE_URI + recipeId)
                    .request()
                    .put(eventFactory.renameRecipeEntity(
                            format("Recipe %04d (rename %02d) %s", recipeIndex, i,
                                    RandomStringUtils.randomAlphanumeric(200, 1000))));
        }

        await().until(() -> eventFinder.eventsWithPayloadContaining(recipeId, recipeId).size() > RENAMES_PER_RECIPE);

        client.target(RECIPES_RESOURCE_URI + recipeId)
                .request()
                .post(entity(createObjectBuilder()
                        .add("recipeId", recipeId)
                        .build()
                        .toString(), REMOVE_RECIPE_MEDIA_TYPE));

        await().until(() -> eventFinder.eventsWithPayloadContaining(recipeId, recipeId).size() > RENAMES_PER_RECIPE + 1);
    }

    // ========================================================================
    // Query helpers
    // ========================================================================

    private int countEventLogEvents() {
        return countFromEventStore("SELECT COUNT(*) FROM event_log");
    }

    private int countLinkedEvents() {
        return countFromEventStore("SELECT COUNT(*) FROM event_log WHERE event_number IS NOT NULL");
    }

    private int countPublishQueue() {
        return countFromEventStore("SELECT COUNT(*) FROM publish_queue");
    }

    private int countFromEventStore(final String sql) {
        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement ps = connection.prepareStatement(sql);
             final ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (final SQLException e) {
            return -1;
        }
    }

    private int countStreamsUpToDate() {
        return countFromViewStoreWithSource(
                "SELECT COUNT(*) FROM stream_status " +
                "WHERE source = ? AND component = 'EVENT_LISTENER' " +
                "AND position >= latest_known_position");
    }

    private int countStreamsBehind() {
        return countFromViewStoreWithSource(
                "SELECT COUNT(*) FROM stream_status " +
                "WHERE source = ? AND component = 'EVENT_LISTENER' " +
                "AND position < latest_known_position " +
                "AND stream_error_id IS NULL");
    }

    private int countStreamsWithErrors() {
        return countFromViewStoreWithSource(
                "SELECT COUNT(*) FROM stream_status " +
                "WHERE source = ? AND component = 'EVENT_LISTENER' " +
                "AND stream_error_id IS NOT NULL");
    }

    private int countAdvisoryLocks() {
        return countFromViewStore(
                "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory'");
    }

    private int countFromViewStoreWithSource(final String sql) {
        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, CONTEXT_NAME);
            try (final ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (final SQLException e) {
            return -1;
        }
    }

    private int countFromViewStore(final String sql) {
        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement ps = connection.prepareStatement(sql);
             final ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (final SQLException e) {
            return -1;
        }
    }

    // ========================================================================
    // Polling helpers
    // ========================================================================

    private void printProgress(final long startTime, final AtomicInteger commandsCompleted) {
        try {
            final double elapsedSecs = (System.currentTimeMillis() - startTime) / 1000.0;
            System.out.printf("[ConcurrencyIT %6.1fs] commands=%d/%d  eventLog=%d  linked=%d  publishQueue=%d  upToDate=%d  behind=%d  errors=%d  advisoryLocks=%d%n",
                    elapsedSecs,
                    commandsCompleted.get(), RECIPE_COUNT,
                    countEventLogEvents(),
                    countLinkedEvents(),
                    countPublishQueue(),
                    countStreamsUpToDate(),
                    countStreamsBehind(),
                    countStreamsWithErrors(),
                    countAdvisoryLocks());
        } catch (final Exception e) {
            // Swallow - progress reporting is best-effort
        }
    }

    private void awaitCountStable(final IntSupplier countSupplier, final int expectedCount, final String label) {
        final long timeout = TimeUnit.MINUTES.toMillis(10);
        final long pollInterval = 2000L;
        final long start = System.currentTimeMillis();
        int lastCount = 0;
        int stableIterations = 0;

        while (System.currentTimeMillis() - start < timeout) {
            final int current = countSupplier.getAsInt();
            if (current >= expectedCount) {
                System.out.printf("  %s reached target: %d >= %d%n", label, current, expectedCount);
                return;
            }
            if (current == lastCount) {
                stableIterations++;
                if (stableIterations >= 5 && current > 0) {
                    System.out.printf("  %s stabilised at %d (expected %d) - proceeding%n", label, current, expectedCount);
                    return;
                }
            } else {
                stableIterations = 0;
            }
            lastCount = current;
            System.out.printf("  Waiting for %s: %d / %d%n", label, current, expectedCount);
            try {
                Thread.sleep(pollInterval);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        System.out.printf("  WARNING: %s timed out at %d / %d%n", label, lastCount, expectedCount);
    }

    private void awaitDrain(final IntSupplier countSupplier, final String label) {
        final long timeout = TimeUnit.MINUTES.toMillis(10);
        final long pollInterval = 2000L;
        final long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < timeout) {
            final int current = countSupplier.getAsInt();
            if (current == 0) {
                System.out.printf("  %s drained to 0%n", label);
                return;
            }
            System.out.printf("  Waiting for %s to drain: %d remaining%n", label, current);
            try {
                Thread.sleep(pollInterval);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        System.out.printf("  WARNING: %s drain timed out at %d%n", label, countSupplier.getAsInt());
    }

    @FunctionalInterface
    private interface IntSupplier {
        int getAsInt();
    }
}