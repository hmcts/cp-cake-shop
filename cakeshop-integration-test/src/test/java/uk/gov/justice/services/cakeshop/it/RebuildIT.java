package uk.gov.justice.services.cakeshop.it;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.DB_CONTEXT_NAME;
import static uk.gov.justice.services.eventstore.management.commands.RebuildCommand.REBUILD;
import static uk.gov.justice.services.jmx.api.mbean.CommandRunMode.GUARDED;
import static uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters.withNoCommandParameters;

import uk.gov.justice.services.cakeshop.it.helpers.CommandSender;
import uk.gov.justice.services.cakeshop.it.helpers.DatabaseManager;
import uk.gov.justice.services.cakeshop.it.helpers.EventFactory;
import uk.gov.justice.services.cakeshop.it.helpers.JmxParametersFactory;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;
import uk.gov.justice.services.jmx.system.command.client.SystemCommanderClient;
import uk.gov.justice.services.jmx.system.command.client.TestSystemCommanderClientFactory;
import uk.gov.justice.services.test.utils.core.messaging.Poller;
import uk.gov.justice.services.test.utils.events.EventStoreDataAccess;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.SequenceSetter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;
import javax.ws.rs.client.Client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RebuildIT {
    private final DataSource eventStoreDataSource = new DatabaseManager().initEventStoreDb();
    private final EventFactory eventFactory = new EventFactory();
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();
    private final EventStoreDataAccess eventStoreDataAccess = new EventStoreDataAccess(eventStoreDataSource);

    private CommandSender commandSender;
    private final SequenceSetter sequenceSetter = new SequenceSetter();

    private final Poller poller = new Poller();

    private final TestSystemCommanderClientFactory testSystemCommanderClientFactory = new TestSystemCommanderClientFactory();

    @BeforeEach
    public void before() throws Exception {
        final Client client = new RestEasyClientFactory().createResteasyClient();
        commandSender = new CommandSender(client, eventFactory);

        databaseCleaner.cleanEventStoreTables(DB_CONTEXT_NAME);
        databaseCleaner.cleanViewStoreTables(DB_CONTEXT_NAME, "cake", "cake_order", "recipe", "ingredient", "processed_event");
    }

    @Test
    public void shouldRenumberTheEventLogTableAndRebuldPublishedEvents() throws Exception {

        final long startNumber = 1000L;
        sequenceSetter.setSequenceTo(startNumber, "event_sequence_seq", eventStoreDataSource);

        commandSender.addRecipe(randomUUID().toString(), "cake 1");
        commandSender.addRecipe(randomUUID().toString(), "cake 2");
        commandSender.addRecipe(randomUUID().toString(), "cake 3");

        final List<PublishedEvent> publishedEvents = getPublishedEvents(startNumber);

        assertThat(publishedEvents.size(), is(3));
        assertThat(publishedEvents.get(0).getEventNumber(), is(of(startNumber)));

        assertThat(eventNumbersLinkedCorrectly(publishedEvents), is(true));

        final List<UUID> eventIds = publishedEvents.stream()
                .map(Event::getId)
                .collect(toList());

        invokeRebuild();

        final long newStartNumber = 1L;
        final List<PublishedEvent> rebuiltEvents = getPublishedEvents(newStartNumber);
        assertThat(rebuiltEvents.size(), is(3));

        final List<UUID> rebuiltEventIds = rebuiltEvents.stream()
                .map(Event::getId)
                .collect(toList());

        assertThat(rebuiltEvents.get(0).getEventNumber(), is(of(newStartNumber)));

        assertThat(rebuiltEventIds, hasItem(eventIds.get(0)));
        assertThat(rebuiltEventIds, hasItem(eventIds.get(1)));
        assertThat(rebuiltEventIds, hasItem(eventIds.get(2)));

        assertThat(eventNumbersLinkedCorrectly(rebuiltEvents), is(true));
    }

    private void invokeRebuild() throws Exception {

        try(final SystemCommanderClient systemCommanderClient = testSystemCommanderClientFactory.create(JmxParametersFactory.buildJmxParameters())) {
            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = withNoCommandParameters();
            systemCommanderClient.getRemote(CONTEXT_NAME).call(
                    REBUILD,
                    jmxCommandRuntimeParameters.getCommandRuntimeId(),
                    jmxCommandRuntimeParameters.getCommandRuntimeString(),
                    GUARDED.isGuarded()
            );
        }
    }

    private List<PublishedEvent> getPublishedEvents(final long startNumber) {

        final Optional<List<PublishedEvent>> publishedEvents = poller.pollUntilFound(() -> {

            final List<PublishedEvent> events = doGetPublishedEvents();

            System.out.printf("Polling published_event table. Expected events count: %d, found: %d\n", 3, events.size());

            if (events.size() == 3) {
                final Optional<Long> eventNumber = events.get(0).getEventNumber();
                if(eventNumber.isPresent()) {
                    if (eventNumber.get() == startNumber) {
                        return of(events);
                    }
                }
            }

            return empty();
        });

        if (publishedEvents.isPresent()) {
            return publishedEvents.get();
        }

        fail();

        return new ArrayList<>();
    }

    private List<PublishedEvent> doGetPublishedEvents()  {
        return eventStoreDataAccess.findAllPublishedEventsOrderedByEventNumber();
    }

    private boolean eventNumbersLinkedCorrectly(final List<PublishedEvent> publishedEvents) {

        long previousEventNumber = 0L;

        for(final PublishedEvent publishedEvent: publishedEvents) {

            if(publishedEvent.getPreviousEventNumber() != previousEventNumber) {
                return false;
            }

            previousEventNumber = publishedEvent.getEventNumber().orElse(-1L);

            if (previousEventNumber == -1L) {

                return false;
            }
        }

        return true;
    }
}
