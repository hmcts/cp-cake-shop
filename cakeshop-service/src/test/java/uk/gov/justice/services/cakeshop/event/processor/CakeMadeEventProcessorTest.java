package uk.gov.justice.services.cakeshop.event.processor;

import static java.time.ZoneOffset.UTC;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;
import static uk.gov.moj.cpp.jobstore.api.task.ExecutionStatus.STARTED;

import uk.gov.justice.services.cakeshop.jobstore.CakeMadeJobData;
import uk.gov.justice.services.common.converter.ObjectToJsonObjectConverter;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.core.sender.Sender;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;
import uk.gov.moj.cpp.jobstore.api.ExecutionService;
import uk.gov.moj.cpp.jobstore.api.task.ExecutionInfo;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.json.JsonObject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class CakeMadeEventProcessorTest {

    @Mock
    private Sender sender;

    @Mock
    private ObjectToJsonObjectConverter objectToJsonObjectConverter;

    @Mock
    private UtcClock clock;

    @Mock
    private ExecutionService executionService;

    @Mock
    private Logger logger;

    @InjectMocks
    private CakeMadeEventProcessor processor;

    @Test
    void shouldSubmitNotificationJobToJobStore() throws Exception{
        final JsonObject payload = mock(JsonObject.class);
        final JsonEnvelope jsonEnvelope = new DefaultJsonEnvelopeProvider().envelopeFrom(metadataBuilder()
                .withId(UUID.randomUUID())
                .withName("cakeshop.events.cake-made")
                .build(), payload);
        final ZonedDateTime now = ZonedDateTime.now(UTC).truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        when(clock.now()).thenReturn(now);
        when(payload.getString("cakeId")).thenReturn("cake-id");
        when(objectToJsonObjectConverter.convert(any())).thenReturn(payload);

        processor.handle(jsonEnvelope);

        verify(executionService).executeWith(argThat((ExecutionInfo executionInfo) -> {
            assertThat(executionInfo.getJobData(), is(payload));
            assertThat(executionInfo.getNextTask(), is("cake-made-notification-task"));
            assertThat(executionInfo.getExecutionStatus(), is(STARTED));
            assertThat(executionInfo.getNextTaskStartTime(), is(now));
            return true;
        }));
        verify(objectToJsonObjectConverter).convert(new CakeMadeJobData("cake-id"));
        verify(sender).send(jsonEnvelope);
    }
}