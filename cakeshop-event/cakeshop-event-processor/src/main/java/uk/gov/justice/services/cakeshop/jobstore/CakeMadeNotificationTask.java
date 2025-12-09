package uk.gov.justice.services.cakeshop.jobstore;

import static uk.gov.justice.services.cakeshop.jobstore.CakeMadeNotificationTask.CAKE_MADE_NOTIFICATION_TASK;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.jms.JmsSender;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;
import uk.gov.justice.services.messaging.spi.DefaultJsonMetadata;
import uk.gov.moj.cpp.jobstore.api.annotation.Task;
import uk.gov.moj.cpp.jobstore.api.task.ExecutableTask;
import uk.gov.moj.cpp.jobstore.api.task.ExecutionInfo;
import uk.gov.moj.cpp.jobstore.api.task.ExecutionStatus;

import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;

@Task(CAKE_MADE_NOTIFICATION_TASK)
public class CakeMadeNotificationTask implements ExecutableTask {

    public static final String CAKE_MADE_NOTIFICATION_TASK = "cake-made-notification-task";

    @Inject
    private JmsSender jmsSender;

    @SuppressWarnings({"squid:S1312"})
    @Inject
    private Logger logger;

    @Inject
    private UtcClock clock;

    @Override
    public ExecutionInfo execute(final ExecutionInfo executionInfo) {
        try{
            final JsonEnvelope jsonEnvelope = new DefaultJsonEnvelopeProvider().envelopeFrom(DefaultJsonMetadata.metadataBuilder()
                            .withId(UUID.randomUUID())
                            .createdAt(clock.now())
                            .withName("jobstore.task.notification.cake-made").build(),
                    executionInfo.getJobData());

            jmsSender.send(jsonEnvelope, "public.event");

            logger.info("Cake made notification sent successfully to public.event topic");

            return ExecutionInfo.executionInfo()
                    .withExecutionStatus(ExecutionStatus.COMPLETED)
                    .build();

        } catch (Exception e) {
            logger.error("Error while sending cake made notification to public.event topic", e);
            return ExecutionInfo.executionInfo()
                    .withExecutionStatus(ExecutionStatus.COMPLETED)
                    .build();
        }
    }
}
