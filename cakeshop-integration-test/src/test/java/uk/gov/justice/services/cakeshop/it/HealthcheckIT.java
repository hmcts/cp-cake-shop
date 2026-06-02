package uk.gov.justice.services.cakeshop.it;

import static com.jayway.jsonassert.JsonAssert.with;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.HEALTHCHECK_URI;

import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthcheckIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthcheckIT.class);

    private final Client client = new RestEasyClientFactory().createResteasyClient();

    @Test
    public void shouldSuccessfullyCallHealthcheckServlet() throws Exception {

        final String healthcheckUri = HEALTHCHECK_URI;

        LOGGER.info("Making request to '" + healthcheckUri + "'");
        final Response response = client.target(healthcheckUri)
                .request()
                .get();

        final String healthcheckJson = response.readEntity(String.class);
        LOGGER.info("HealthcheckJSON: " + healthcheckJson);

        assertThat(response.getStatus(), is(200));
        with(healthcheckJson)
                .assertThat("$.allHealthchecksPassed", is(true))
                .assertThat("$.healthcheckRunDetails[*].healthcheckName", hasItems("system-database-healthcheck",
                        "artemis-healthcheck", "event-store-healthcheck", "view-store-healthcheck", "file-store-healthcheck"));
    }
}
