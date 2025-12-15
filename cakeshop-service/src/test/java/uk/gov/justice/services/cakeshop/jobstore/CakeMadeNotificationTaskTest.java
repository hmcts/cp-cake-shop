package uk.gov.justice.services.cakeshop.jobstore;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.converter.ObjectToJsonObjectConverter;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.messaging.jms.JmsSender;
import uk.gov.moj.cpp.jobstore.api.task.ExecutionInfo;
import uk.gov.moj.cpp.jobstore.api.task.ExecutionStatus;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import javax.json.JsonObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class CakeMadeNotificationTaskTest {

    private static final ObjectToJsonObjectConverter objectToJsonObjectConverter = new ObjectToJsonObjectConverter(new ObjectMapper());

    @Mock
    private JmsSender jmsSender;

    @Mock
    private UtcClock clock;

    @Mock
    private Logger logger;

    @InjectMocks
    private CakeMadeNotificationTask task;

    @Test
    void shouldSendJmsNotificationOnSuccessfulExecution() {
        final JsonObject jobData = mock(JsonObject.class);
        final ExecutionInfo executionInfo = ExecutionInfo.executionInfo()
                .withJobData(jobData)
                .build();
        final ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS);
        when(clock.now()).thenReturn(now);

        final ExecutionInfo result = task.execute(executionInfo);

        assertThat(result.getExecutionStatus(), is(ExecutionStatus.COMPLETED));
        verify(jmsSender).send(argThat(jsonEnvelope -> {
            assertThat(jsonEnvelope.payloadAsJsonObject(), is(jobData));
            assertThat(jsonEnvelope.metadata().name(), is("jobstore.task.notification.cake-made"));
            assertNotNull(jsonEnvelope.metadata().id());
            assertThat(jsonEnvelope.metadata().createdAt().get(), is(now));

            return true;
        }), eq("public.event"));
    }

    @Test
    void shouldNotSendJmsNotificationOnErrorAndMarkJobAsCompleted() {
        final ExecutionInfo result = task.execute(null);

        assertThat(result.getExecutionStatus(), is(ExecutionStatus.COMPLETED));
        verifyNoInteractions(jmsSender);
    }
}