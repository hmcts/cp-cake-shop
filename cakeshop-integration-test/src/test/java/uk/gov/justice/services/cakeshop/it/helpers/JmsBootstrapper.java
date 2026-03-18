package uk.gov.justice.services.cakeshop.it.helpers;

import com.jayway.jsonpath.JsonPath;
import java.util.Optional;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import uk.gov.justice.services.test.utils.core.messaging.Poller;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;

public class JmsBootstrapper {

    private static final String JMS_USERNAME = SystemPropertyFinder.findJmsUserName();
    private static final String JMS_PASSWORD = SystemPropertyFinder.findJmsUserPassword();
    private static final String JMS_PORT = SystemPropertyFinder.findJmsPort();
    private static final String JMS_HOST = System.getProperty("INTEGRATION_HOST_KEY", "localhost");
    private static final String JMS_BROKER_URL = "tcp://" + JMS_HOST + ":" + JMS_PORT;
    private final Poller poller = new Poller();

    private final ActiveMQConnectionFactory jmsConnectionFactory = new ActiveMQConnectionFactory(JMS_BROKER_URL);


    public Session jmsSession() throws JMSException {
        final Connection connection = jmsConnectionFactory.createConnection(JMS_USERNAME, JMS_PASSWORD);
        connection.start();
        return connection.createSession(false, AUTO_ACKNOWLEDGE);
    }

    public MessageConsumer topicConsumerOf(final String topicName, final Session session) throws JMSException {
        final Topic topic = session.createTopic(topicName);
        return session.createConsumer(topic);
    }

    public void clearDeadLetterQueue() throws Exception {
        try (final Session jmsSession = jmsSession();) {
            final MessageConsumer dlqConsumer = queueConsumerOf("DLQ", jmsSession);
            clear(dlqConsumer);
        }
    }

    public MessageConsumer queueConsumerOf(final String queueName, final Session session) throws JMSException {
        final Queue queue = session.createQueue(queueName);
        return session.createConsumer(queue);
    }

    public QueueBrowser queueBrowserOf(final String queueName, final Session session) throws JMSException {
        final Queue queue = session.createQueue(queueName);
        return session.createBrowser(queue);
    }

    public void clear(MessageConsumer msgConsumer) throws JMSException {
        while (msgConsumer.receiveNoWait() != null) {
        }
    }

    public Optional<String> getPayloadByEventName(final MessageConsumer messageConsumerClient, final String expectedEventName) {
        return poller.pollUntilFound(() -> {
                    final Optional<String> eventPayload = retrieveEventPayload(messageConsumerClient);
                    final boolean eventMatched = eventPayload
                            .map(payload -> JsonPath.parse(payload).read("$._metadata.name", String.class))
                            .filter(eventName -> eventName.equals(expectedEventName))
                            .isPresent();

                    return eventMatched ? eventPayload : Optional.empty();
        });
    }

    private Optional<String> retrieveEventPayload(final MessageConsumer messageConsumerClient) {
        try {
            final Message message = messageConsumerClient.receive(1000*5);

            return message != null ? Optional.ofNullable(((TextMessage) message).getText()) : Optional.empty();
        } catch (JMSException e) {
            return Optional.empty();
        }
    }
}
