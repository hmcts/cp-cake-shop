package uk.gov.justice.services.cakeshop.it;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;
import static uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand.CATCHUP;
import static uk.gov.justice.services.jmx.api.mbean.CommandRunMode.GUARDED;
import static uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters.withNoCommandParameters;

import uk.gov.justice.services.cakeshop.it.helpers.BatchEventInserter;
import uk.gov.justice.services.cakeshop.it.helpers.CakeshopEventGenerator;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.JmxParametersFactory;
import uk.gov.justice.services.cakeshop.it.helpers.PositionInStreamIterator;
import uk.gov.justice.services.cakeshop.it.helpers.ProcessedEventFinder;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;
import uk.gov.justice.services.jmx.system.command.client.SystemCommanderClient;
import uk.gov.justice.services.jmx.system.command.client.TestSystemCommanderClientFactory;
import uk.gov.justice.services.test.utils.core.messaging.Poller;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CatchupPerformanceIT {

    private static final int BATCH_INSERT_SIZE = 10_000;

    private final DataSource eventStoreDataSource = new DatabaseManager().initEventStoreDb();
    private final DataSource viewStoreDataSource = new DatabaseManager().initViewStoreDb();

    private final ProcessedEventFinder processedEventCounter = new ProcessedEventFinder(viewStoreDataSource);

    private final TestSystemCommanderClientFactory systemCommanderClientFactory = new TestSystemCommanderClientFactory();
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

    private final Poller longPoller = new Poller(2400, 1000L);
    private final BatchEventInserter batchEventInserter = new BatchEventInserter(eventStoreDataSource, BATCH_INSERT_SIZE);

    @BeforeEach
    public void before() {
        databaseCleaner.cleanEventStoreTables(CONTEXT_NAME);
        cleanViewstoreTables();
        databaseCleaner.cleanSystemTables(CONTEXT_NAME);
    }

    @Test
    public void shouldReplayAndFindRecipesInViewStore() throws Exception {

        final int numberOfStreams = 10;
        final int numberOfEventsPerStream = 100;
        final int totalEvents = numberOfStreams * numberOfEventsPerStream;

        System.out.println(format(
                "Inserting %d events into event_log (%d events in %d streams)",
                totalEvents,
                numberOfEventsPerStream,
                numberOfStreams
        ));

        addEventsToEventLog(numberOfStreams, numberOfEventsPerStream);

        System.out.println("Inserted " + totalEvents + " events into event_log");
        System.out.println("Waiting for events to publish...");

        final Optional<Integer> processedEventCount = longPoller.pollUntilFound(() -> {
            final int eventCount = processedEventCounter.countProcessedEventsForEventListener();
            System.out.printf("Polling processed_event table. Expected events count: %d, found: %d\n", totalEvents, eventCount);
            if (eventCount == totalEvents) {
                return of(eventCount);
            }

            return empty();
        });

        assertThat(processedEventCount, is(of(totalEvents)));


        cleanViewstoreTables();

        System.out.println("Running catchup...");
        runCatchup();

        final Optional<Integer> numberOfReplayedEvents = longPoller.pollUntilFound(() -> {
            final int eventCount = processedEventCounter.countProcessedEventsForEventListener();
            System.out.printf("Polling processed_event table. Expected events count: %d, found: %d\n", totalEvents, eventCount);

            if (eventCount == totalEvents) {
                return of(eventCount);
            }

            return empty();
        });

        if (numberOfReplayedEvents.isPresent()) {
            System.out.println("Successfully caught up " + numberOfReplayedEvents.get() + " events");
        } else {
            fail("Failed to catchup " + totalEvents + " events.");
        }
    }

    private void addEventsToEventLog(final int numberOfStreams, final int numberOfEventsPerStream) throws Exception {

        final CakeshopEventGenerator cakeshopEventGenerator = new CakeshopEventGenerator();

        final List<Event> events = new ArrayList<>();
        final List<UUID> streamIds = new ArrayList<>();

        for (int seed = 0; seed < numberOfStreams; seed++) {

            final PositionInStreamIterator positionInStreamIterator = new PositionInStreamIterator();

            final Event recipeAddedEvent = cakeshopEventGenerator.createRecipeAddedEvent(seed, positionInStreamIterator);
            final UUID recipeId = recipeAddedEvent.getStreamId();

            if (!streamIds.contains(recipeId)) {
                streamIds.add(recipeId);
            }

            events.add(recipeAddedEvent);

            for (int renameNumber = 1; renameNumber < numberOfEventsPerStream; renameNumber++) {
                final Event recipeRenamedEvent = cakeshopEventGenerator.createRecipeRenamedEvent(recipeId, seed, renameNumber, positionInStreamIterator);
                events.add(recipeRenamedEvent);
            }
        }

        batchEventInserter.updateEventStreamTable(streamIds);
        batchEventInserter.updateEventLogTable(events);
    }

    private void runCatchup() throws Exception {

        try (final SystemCommanderClient systemCommanderClient = systemCommanderClientFactory.create(JmxParametersFactory.buildJmxParameters())) {

            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = withNoCommandParameters();
            systemCommanderClient.getRemote(CONTEXT_NAME).call(
                    CATCHUP,
                    jmxCommandRuntimeParameters.getCommandRuntimeId(),
                    jmxCommandRuntimeParameters.getCommandRuntimeString(),
                    GUARDED.isGuarded());
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
}
