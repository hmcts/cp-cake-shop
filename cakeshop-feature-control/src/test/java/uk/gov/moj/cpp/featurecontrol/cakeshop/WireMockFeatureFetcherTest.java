package uk.gov.moj.cpp.featurecontrol.cakeshop;

import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.moj.cpp.featurecontrol.cakeshop.WireMockFeatureFetcher.FEATURES_URL;

import uk.gov.justice.services.core.featurecontrol.domain.Feature;

import java.util.List;

import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WireMockFeatureFetcherTest {

    @Mock
    private Client client;

    @Mock
    private WebTarget webTarget;

    @Mock
    private Invocation.Builder invocationBuilder;

    @Mock
    private Response response;

    @InjectMocks
    private WireMockFeatureFetcher wireMockFeatureFetcher;

    @Test
    public void shouldFetchTheFeaturesFromTheFeatureStoreStub() {

        final JsonObject featuresJson = createObjectBuilder()
                .add("features", createArrayBuilder()
                        .add(createObjectBuilder().add("featureName", "feature-1").add("enabled", true))
                        .add(createObjectBuilder().add("featureName", "feature-2").add("enabled", false)))
                .build();

        featureStoreStubReturns(200);
        when(response.readEntity(JsonObject.class)).thenReturn(featuresJson);

        final List<Feature> features = wireMockFeatureFetcher.fetchFeatures();

        assertThat(features, contains(new Feature("feature-1", true), new Feature("feature-2", false)));
        verify(response).close();
    }

    @Test
    public void shouldThrowIfTheFeatureStoreStubReturnsAnHttpStatusOtherThan200() {

        featureStoreStubReturns(503);

        final FeatureFetchException featureFetchException = assertThrows(FeatureFetchException.class, () ->
                wireMockFeatureFetcher.fetchFeatures());

        assertThat(featureFetchException.getMessage(), is("Failed to fetch features; url " + FEATURES_URL + " returned HTTP status 503"));
        verify(response).close();
    }

    @Test
    public void shouldCloseTheClient() {

        wireMockFeatureFetcher.closeClient();

        verify(client).close();
    }

    private void featureStoreStubReturns(final int httpStatus) {
        when(client.target(FEATURES_URL)).thenReturn(webTarget);
        when(webTarget.request(APPLICATION_JSON)).thenReturn(invocationBuilder);
        when(invocationBuilder.get()).thenReturn(response);
        when(response.getStatus()).thenReturn(httpStatus);
    }
}
