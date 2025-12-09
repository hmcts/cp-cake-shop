package uk.gov.justice.services.cakeshop.event.processor;

import static uk.gov.justice.services.cakeshop.jobstore.CakeMadeNotificationTask.CAKE_MADE_NOTIFICATION_TASK;
import static uk.gov.justice.services.core.annotation.Component.EVENT_PROCESSOR;
import static uk.gov.moj.cpp.jobstore.api.task.ExecutionStatus.STARTED;
import static uk.gov.moj.cpp.jobstore.persistence.Priority.HIGH;

import uk.gov.justice.services.cakeshop.jobstore.CakeMadeJobData;
import uk.gov.justice.services.common.converter.ObjectToJsonObjectConverter;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.sender.Sender;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.moj.cpp.jobstore.api.ExecutionService;
import uk.gov.moj.cpp.jobstore.api.task.ExecutionInfo;

import javax.inject.Inject;
import javax.json.JsonObject;

import org.slf4j.Logger;

@ServiceComponent(EVENT_PROCESSOR)
public class CakeMadeEventProcessor {

    @Inject
    Sender sender;

    @Inject
    private ObjectToJsonObjectConverter objectToJsonObjectConverter;

    @Inject
    private UtcClock clock;

    @Inject
    private ExecutionService executionService;

    @SuppressWarnings({"squid:S1312"})
    @Inject
    private Logger logger;

    @Handles("cakeshop.events.cake-made")
    public void handle(final JsonEnvelope event) {
        final JsonObject cakeMadeEventPayload = event.payloadAsJsonObject();
        final String cakeId = cakeMadeEventPayload.getString("cakeId");
        final ExecutionInfo executionInfo = new ExecutionInfo(
                objectToJsonObjectConverter.convert(new CakeMadeJobData(cakeId)),
                CAKE_MADE_NOTIFICATION_TASK,
                clock.now(),
                STARTED,
                HIGH);

        executionService.executeWith(executionInfo);

        logger.info("Cake made notification task submitted to job store");

        sender.send(event);
    }
}
