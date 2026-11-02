package uk.gov.moj.cpp.featurecontrol.cakeshop;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.client.ClientBuilder.newClient;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.OK;

import uk.gov.justice.services.core.featurecontrol.FeatureFetcher;
import uk.gov.justice.services.core.featurecontrol.domain.Feature;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

/**
 * Fetches the remote Features from a WireMock stub, standing in for Azure App Configuration, so
 * the integration tests can control what the remote feature store returns, including failures.
 *
 * The stub returns {"features": [{"featureName": "...", "enabled": true}]}. Any other HTTP
 * status throws a {@link FeatureFetchException}, as a failed call to Azure does.
 */
@Alternative
@Priority(1)
public class WireMockFeatureFetcher implements FeatureFetcher {

    public static final String FEATURES_URL = "http://localhost:8080/cakeshop-feature-store/features";

    private Client client;

    @PostConstruct
    public void createClient() {
        client = newClient();
    }

    @Override
    public List<Feature> fetchFeatures() {
        try (final Response response = client.target(FEATURES_URL).request(APPLICATION_JSON).get()) {
            if (response.getStatus() == OK.getStatusCode()) {
                return featuresFrom(response.readEntity(JsonObject.class));
            }

            throw new FeatureFetchException(format("Failed to fetch features; url %s returned HTTP status %d", FEATURES_URL, response.getStatus()));
        }
    }

    @PreDestroy
    public void closeClient() {
        client.close();
    }

    private List<Feature> featuresFrom(final JsonObject featuresJson) {
        return featuresJson.getJsonArray("features")
                .getValuesAs(JsonObject.class)
                .stream()
                .map(featureJson -> new Feature(featureJson.getString("featureName"), featureJson.getBoolean("enabled")))
                .collect(toList());
    }
}
