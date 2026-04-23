package uk.gov.justice.services.cakeshop.command.api;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static uk.gov.justice.services.core.annotation.Component.COMMAND_API;
import static uk.gov.justice.services.test.utils.core.matchers.HandlerClassMatcher.isHandlerClass;
import static uk.gov.justice.services.test.utils.core.matchers.HandlerMethodMatcher.method;
import static uk.gov.justice.services.test.utils.core.messaging.JsonEnvelopeBuilder.envelope;
import static uk.gov.justice.services.test.utils.core.messaging.MetadataBuilderFactory.metadataWithDefaults;

import uk.gov.justice.services.core.sender.Sender;
import uk.gov.justice.services.messaging.Envelope;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.spi.DefaultEnvelope;

import jakarta.json.JsonObject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MakeCakeCommandApiTest {

    @Mock
    private Sender sender;

    @InjectMocks
    private MakeCakeCommandApi commandApi;

    @Captor
    private ArgumentCaptor<DefaultEnvelope> envelopeCaptor;

    @Test
    public void shouldHandleMakeCakeCommand() throws Exception {
        assertThat(MakeCakeCommandApi.class, isHandlerClass(COMMAND_API)
                .with(method("handle")
                        .thatHandles("cakeshop.make-cake")));
    }

    @Test
    public void shouldHandleMakeCakeRequest() {
        commandApi = new MakeCakeCommandApi();
        commandApi.sender = sender;

        final JsonEnvelope envelope = envelope()
                .with(metadataWithDefaults().withName("cakeshop.make-cake"))
                .withPayloadOf("Field", "Value").build();

        final Envelope<JsonObject> jsonObjectEnvelope = commandApi.handle(envelope);

        assertThat(jsonObjectEnvelope.payload().getString("status"), equalTo("Making Cake"));

        verify(sender).send(envelopeCaptor.capture());
        assertThat(envelopeCaptor.getValue().metadata().name(), is("cakeshop.command.make-cake"));
    }
}
