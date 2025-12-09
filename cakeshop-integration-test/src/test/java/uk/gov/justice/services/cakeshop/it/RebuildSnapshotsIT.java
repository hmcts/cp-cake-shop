package uk.gov.justice.services.cakeshop.it;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;
import static uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands.RebuildSnapshotCommand.REBUILD_SNAPSHOTS;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_COMPLETE;
import static uk.gov.justice.services.jmx.api.mbean.CommandRunMode.FORCED;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;

import uk.gov.justice.domain.snapshot.AggregateSnapshot;
import uk.gov.justice.services.cakeshop.domain.aggregate.Recipe;
import uk.gov.justice.services.cakeshop.it.helpers.BatchEventInserter;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.JmxParametersFactory;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.jdbc.snapshot.SnapshotJdbcRepository;
import uk.gov.justice.services.eventsourcing.jdbc.snapshot.StandaloneSnapshotJdbcRepositoryFactory;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.util.jee.timer.StopWatchFactory;
import uk.gov.justice.services.jmx.api.domain.CommandState;
import uk.gov.justice.services.jmx.api.domain.SystemCommandStatus;
import uk.gov.justice.services.jmx.api.mbean.SystemCommanderMBean;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;
import uk.gov.justice.services.jmx.system.command.client.SystemCommanderClient;
import uk.gov.justice.services.jmx.system.command.client.TestSystemCommanderClientFactory;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.test.utils.core.messaging.Poller;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.json.JsonArrayBuilder;
import javax.sql.DataSource;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RebuildSnapshotsIT {

    private static final String EVENT_SOURCE = "cakeshop";
    private static final int NUMBER_OF_EVENTS_ON_STREAM_TO_CREATE = 100_000;

    private final TestSystemCommanderClientFactory systemCommanderClientFactory = new TestSystemCommanderClientFactory();
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();
    private final UtcClock clock = new UtcClock();
    private final DataSource eventStoreDataSource = new DatabaseManager().initEventStoreDb();
    private final SnapshotJdbcRepository snapshotJdbcRepository = new StandaloneSnapshotJdbcRepositoryFactory().getSnapshotJdbcRepository(eventStoreDataSource);
    private final BatchEventInserter batchEventInserter = new BatchEventInserter(eventStoreDataSource, 10_000);
    private final Poller longPoller = new Poller(2400, 1000L);

    @BeforeEach
    public void cleanDatabase() {
        databaseCleaner.cleanEventStoreTables(CONTEXT_NAME);
        cleanViewstoreTables();
        databaseCleaner.cleanSystemTables(CONTEXT_NAME);
    }

    @Test
    public void shouldUseJmxCommandToHydrateAggregateAndStoreSnapshot() throws Exception {

        final String aggregateClassName = Recipe.class.getName();
        final UUID recipeId = randomUUID();

        snapshotJdbcRepository.removeAllSnapshots(recipeId, Recipe.class);
        assertThat(snapshotJdbcRepository.getLatestSnapshot(recipeId, Recipe.class), is(empty()));

        System.out.println("Generating " + NUMBER_OF_EVENTS_ON_STREAM_TO_CREATE + " events");
        final List<Event> events = createEvents(NUMBER_OF_EVENTS_ON_STREAM_TO_CREATE, recipeId);
        System.out.println("Inserting " + NUMBER_OF_EVENTS_ON_STREAM_TO_CREATE + " events into event_log table");

        batchEventInserter.updateEventLogTable(events);

        final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = new JmxCommandRuntimeParameters.JmxCommandRuntimeParametersBuilder()
                .withCommandRuntimeString(aggregateClassName)
                .withCommandRuntimeId(recipeId)
                .build();

        try (final SystemCommanderClient systemCommanderClient = systemCommanderClientFactory.create(JmxParametersFactory.buildJmxParameters())) {
            final SystemCommanderMBean systemCommanderMBean = systemCommanderClient.getRemote(CONTEXT_NAME);
            final UUID commandId = systemCommanderMBean.call(
                    REBUILD_SNAPSHOTS,
                    jmxCommandRuntimeParameters.getCommandRuntimeId(),
                    jmxCommandRuntimeParameters.getCommandRuntimeString(),
                    FORCED.isGuarded());

            final StopWatch stopWatch = new StopWatchFactory().createStartedStopWatch();
            final Optional<SystemCommandStatus> systemCommandStatus = longPoller.pollUntilFound(() -> {
                final SystemCommandStatus commandStatus = systemCommanderMBean.getCommandStatus(commandId);
                final String systemCommandName = commandStatus.getSystemCommandName();
                final CommandState commandState = commandStatus.getCommandState();

                System.out.println("JMX command '" + systemCommandName + "' state is '" + commandStatus.getCommandState() + "'");

                if (commandState.equals(COMMAND_COMPLETE)) {
                    return Optional.of(commandStatus);
                }

                return empty();
            });

            stopWatch.stop();

            if (systemCommandStatus.isPresent()) {
                assertThat(systemCommandStatus.get().getCommandState(), is(COMMAND_COMPLETE));
                final Optional<AggregateSnapshot<Recipe>> latestSnapshot = snapshotJdbcRepository.getLatestSnapshot(
                        recipeId,
                        Recipe.class);

                assertThat(latestSnapshot.isPresent(), is(true));

                System.out.println("Run of JMX command '" + REBUILD_SNAPSHOTS + "' took " + stopWatch.getTime() + " milliseconds");

            } else {
                fail();
            }
        }
    }

    private void cleanViewstoreTables() {
        databaseCleaner.cleanViewStoreTables(CONTEXT_NAME,
                "ingredient",
                "recipe",
                "cake",
                "cake_order",
                "processed_event"
        );
        databaseCleaner.cleanStreamBufferTable(CONTEXT_NAME);
        databaseCleaner.cleanStreamStatusTable(CONTEXT_NAME);
    }

    private List<Event> createEvents(final long numberToCreate, final UUID streamId) {

        final long firstPositionInStream = 1;
        final ArrayList<Event> events = new ArrayList<>();
        final Event recipeAddedEvent = createRecipeAddedEvent(firstPositionInStream, streamId);
        events.add(recipeAddedEvent);

        for (int posistionInStream = 2; posistionInStream <= numberToCreate; posistionInStream++) {

            final Event recipeRenamedEvent = createRecipeRenamedEvent(streamId, posistionInStream);
            events.add(recipeRenamedEvent);
        }

        return events;
    }

    private Event createRecipeAddedEvent(final long positionInStream, final UUID streamId) {

        final ZonedDateTime now = clock.now();
        final Metadata metadata = metadataBuilder()
                .createdAt(now)
                .withId(randomUUID())
                .withName("cakeshop.events.recipe-added")
                .withStreamId(streamId)
                .withPosition(positionInStream)
                .withSource(EVENT_SOURCE)
                .build();

        final JsonArrayBuilder ingredients = createArrayBuilder();

        for (int i = 0; i < 3; i++) {
            ingredients.add(createObjectBuilder()
                    .add("name", "ingredient " + i)
                    .add("quantity", i));
        }

        final String recipe = createObjectBuilder()
                .add("recipeId", streamId.toString())
                .add("name", format("Recipe %d", positionInStream))
                .add("ingredients", ingredients.build())
                .add("glutenFree", false)
                .build()
                .toString();

        return new Event(
                randomUUID(),
                streamId,
                positionInStream,
                "cakeshop.events.recipe-added",
                metadata.asJsonObject().toString(),
                recipe,
                now);
    }

    private Event createRecipeRenamedEvent(final UUID recipeId, final long positionInStream) {

        final ZonedDateTime now = clock.now();


        final Metadata metadata = metadataBuilder()
                .createdAt(now)
                .withId(randomUUID())
                .withName("cakeshop.events.recipe-renamed")
                .withStreamId(recipeId)
                .withPosition(positionInStream)
                .withSource(EVENT_SOURCE)
                .build();

        final String recipeRename = createObjectBuilder()
                .add("recipeId", recipeId.toString())
                .add("name", format("Recipe %d", positionInStream))
                .build()
                .toString();

        return new Event(
                randomUUID(),
                recipeId,
                positionInStream,
                "cakeshop.events.recipe-renamed",
                metadata.asJsonObject().toString(),
                recipeRename,
                now);
    }
}
