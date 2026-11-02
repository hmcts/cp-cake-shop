package uk.gov.justice.services.cakeshop.it;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.findAll;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.resetAllRequests;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER;
import static java.lang.Integer.parseInt;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static jakarta.json.Json.createArrayBuilder;
import static jakarta.json.Json.createObjectBuilder;
import static jakarta.ws.rs.client.Entity.entity;
import static jakarta.ws.rs.core.Response.Status.ACCEPTED;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static uk.gov.justice.services.cakeshop.it.helpers.SystemPropertyFinder.findWildflyHttpPort;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopMediaTypes.ADD_RECIPE_VERSION_2_MEDIA_TYPE;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.RECIPES_RESOURCE_URI;
import static uk.gov.justice.services.test.utils.common.host.TestHostProvider.getHost;

import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * PEG-3655: a failed refresh of the remote Features must not switch Features off.
 *
 * cake-shop fetches its remote Features from a WireMock stub (see WireMockFeatureFetcher) with
 * the feature cache enabled and refreshed every 2 seconds (see the service's ejb-jar.xml). The
 * 'recipes-have-allergens-specified' Feature guards the add-recipe-v2 command: 202 ACCEPTED when
 * it is enabled, 403 FORBIDDEN when it is disabled.
 */
public class FeatureControlRefreshFailureIT {

    private static final String FEATURE_NAME = "recipes-have-allergens-specified";
    private static final String FEATURES_PATH = "/cakeshop-feature-store/features";

    private Client client;

    @BeforeEach
    public void createRestClientAndConfigureWireMock() {
        client = new RestEasyClientFactory().createResteasyClient();
        configureFor(getHost(), parseInt(findWildflyHttpPort()));
    }

    @AfterEach
    public void restoreTheDefaultFeaturesAndCloseRestClient() {
        featureStoreReturnsTheFeatureAs(false);
        client.close();
    }

    @Test
    public void shouldKeepAFeatureEnabledWhenRefreshingTheFeaturesFailsWithAnHttpError() {

        featureStoreReturnsTheFeatureAs(true);
        await().atMost(30, SECONDS).until(() -> addRecipeVersion2Status() == ACCEPTED.getStatusCode());

        featureStoreReturns(aResponse().withStatus(503));
        waitForTheFeaturesToBeRefreshedTwice();

        assertThat(addRecipeVersion2Status(), is(ACCEPTED.getStatusCode()));
    }

    @Test
    public void shouldKeepAFeatureEnabledWhenTheFeatureStoreCannotBeReached() {

        featureStoreReturnsTheFeatureAs(true);
        await().atMost(30, SECONDS).until(() -> addRecipeVersion2Status() == ACCEPTED.getStatusCode());

        featureStoreReturns(aResponse().withFault(CONNECTION_RESET_BY_PEER));
        waitForTheFeaturesToBeRefreshedTwice();

        assertThat(addRecipeVersion2Status(), is(ACCEPTED.getStatusCode()));
    }

    @Test
    public void shouldDisableAFeatureWhenTheFeatureStoreRecoversWithTheFeatureDisabled() {

        featureStoreReturnsTheFeatureAs(true);
        await().atMost(30, SECONDS).until(() -> addRecipeVersion2Status() == ACCEPTED.getStatusCode());

        featureStoreReturns(aResponse().withStatus(503));
        waitForTheFeaturesToBeRefreshedTwice();

        featureStoreReturnsTheFeatureAs(false);
        await().atMost(30, SECONDS).until(() -> addRecipeVersion2Status() == FORBIDDEN.getStatusCode());
    }

    private int addRecipeVersion2Status() {
        final String recipe = createObjectBuilder()
                .add("name", "Allergen Aware Cake")
                .add("glutenFree", false)
                .add("ingredients", createArrayBuilder()
                        .add(createObjectBuilder()
                                .add("name", "someIngredient")
                                .add("quantity", 1)))
                .build()
                .toString();

        final Response response = client.target(RECIPES_RESOURCE_URI + randomUUID())
                .request()
                .post(entity(recipe, ADD_RECIPE_VERSION_2_MEDIA_TYPE));
        response.close();

        return response.getStatus();
    }

    private void featureStoreReturnsTheFeatureAs(final boolean enabled) {
        final String features = createObjectBuilder()
                .add("features", createArrayBuilder()
                        .add(createObjectBuilder()
                                .add("featureName", FEATURE_NAME)
                                .add("enabled", enabled)))
                .build()
                .toString();

        featureStoreReturns(okJson(features));
    }

    private void featureStoreReturns(final ResponseDefinitionBuilder response) {
        stubFor(get(urlPathEqualTo(FEATURES_PATH)).willReturn(response));
        resetAllRequests();
    }

    /**
     * A refresh that was already in flight when the stub changed may still have had the old
     * response, so wait for two refreshes to be sure at least one got the new one.
     */
    private void waitForTheFeaturesToBeRefreshedTwice() {
        await().atMost(30, SECONDS).until(() -> findAll(getRequestedFor(urlPathEqualTo(FEATURES_PATH))).size(), greaterThanOrEqualTo(2));
    }
}
