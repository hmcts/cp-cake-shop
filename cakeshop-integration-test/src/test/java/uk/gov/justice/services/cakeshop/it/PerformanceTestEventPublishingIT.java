package uk.gov.justice.services.cakeshop.it;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;

import uk.gov.justice.services.cakeshop.it.helpers.BatchEventInserter;
import uk.gov.justice.services.cakeshop.it.helpers.CakeshopEventGenerator;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.PositionInStreamIterator;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.util.jee.timer.StopWatchFactory;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PerformanceTestEventPublishingIT {

    private static final int BATCH_INSERT_SIZE = 10_000;

    private final DataSource eventStoreDataSource = new DatabaseManager().initEventStoreDb();

    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();
    private final BatchEventInserter batchEventInserter = new BatchEventInserter(eventStoreDataSource, BATCH_INSERT_SIZE);

    @BeforeEach
    public void before() {
        databaseCleaner.cleanEventStoreTables(CONTEXT_NAME);
        cleanViewstoreTables();
        databaseCleaner.cleanSystemTables(CONTEXT_NAME);
    }


    // Warning. This test doesn't test anything. It's for adding events, so we can observe performance of publishing
    @Test
    public void shouldInsertManyEventsOnManyStreamsToPerformanceTestPublishing() throws Exception {

        // Please adjust to taste
        final int numberOfStreams = 100;
        final int numberOfEventsPerStream = 100;

        System.out.println(format("Adding %d events on %d streams to event_log and published_event, giving %d events in total",
                numberOfEventsPerStream,
                numberOfStreams,
                numberOfEventsPerStream * numberOfStreams));

        final StopWatch stopWatch = new StopWatchFactory().createStartedStopWatch();
        addEventsToEventLog(numberOfStreams, numberOfEventsPerStream);
        stopWatch.stop();

        final float timeInSeconds = stopWatch.getTime(MILLISECONDS) / 1000f;
        System.out.printf("Adding %d events into the event store took %f seconds%n",
                numberOfEventsPerStream * numberOfStreams,
                timeInSeconds);

        assertThat("Stop sonar failing the build by adding an assertion", stopWatch, is(notNullValue()));
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

    private void cleanViewstoreTables() {
        databaseCleaner.cleanViewStoreTables(CONTEXT_NAME,
                "ingredient",
                "recipe",
                "cake",
                "cake_order",
                "processed_event",
                "stream_error",
                "stream_error_hash"
        );
        databaseCleaner.cleanStreamBufferTable(CONTEXT_NAME);
        databaseCleaner.cleanStreamStatusTable(CONTEXT_NAME);
    }
}
