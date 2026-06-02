package uk.gov.justice.services.cakeshop.it.helpers;

import static java.lang.String.format;

import uk.gov.justice.services.messaging.JsonEnvelope;

import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

public class EfficientEventSender implements AutoCloseable {

    private final Session jmsSession;
    private final MessageProducer messageProducer;

    private EfficientEventSender(final Session jmsSession, final MessageProducer messageProducer) {
        this.jmsSession = jmsSession;
        this.messageProducer = messageProducer;
    }

    public static EfficientEventSender createFor(final String topicName)  {

        try {
            final Session jmsSession = new JmsBootstrapper().jmsSession();
            final Topic topic = jmsSession.createTopic(topicName);
            final MessageProducer messageProducer = jmsSession.createProducer(topic);

            return new EfficientEventSender(jmsSession, messageProducer);
        } catch (final JMSException e) {
            throw new EventSenderSetupException(format("Failed to create EfficientEventSender for '%s' topic", topicName), e);
        }
    }

    public void sendToTopic(final JsonEnvelope jsonEnvelope) throws JMSException {

        final String json = jsonEnvelope.asJsonObject().toString();
        final TextMessage message = jmsSession.createTextMessage();

        message.setText(json);
        message.setStringProperty("CPPNAME", jsonEnvelope.metadata().name());

        messageProducer.send(message);
    }

    @Override
    public void close() {
        doClose(messageProducer);
        doClose(jmsSession);
    }

    private void doClose(final AutoCloseable autoCloseable) {
        try {
            if (autoCloseable != null) {
                autoCloseable.close();
            }
        } catch (final Exception ignored) {
        }
    }

    public static class EventSenderSetupException extends RuntimeException {
        public EventSenderSetupException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
