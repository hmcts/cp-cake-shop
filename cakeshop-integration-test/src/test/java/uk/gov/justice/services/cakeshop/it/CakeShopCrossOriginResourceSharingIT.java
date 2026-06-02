package uk.gov.justice.services.cakeshop.it;

import static java.util.Arrays.asList;
import static jakarta.ws.rs.core.Response.Status.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsNull.nullValue;
import static uk.gov.justice.services.cakeshop.it.params.CakeShopUris.ORDERS_RESOURCE_URI;

import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CakeShopCrossOriginResourceSharingIT {

    private Client client;

    @BeforeEach
    public void before() throws Exception {
        client = new RestEasyClientFactory().createResteasyClient();
    }

    @AfterEach
    public void cleanup() throws Exception {
        client.close();
    }

    @Test
    public void shouldReturnCORSResponse() {
        final Response corsResponse =
                client.target(ORDERS_RESOURCE_URI + "123")
                        .request()
                        .header("Origin", "http://foo.example")
                        .header("Access-Control-Request-Headers", "CPPCLIENTCORRELATIONID")
                        .options();

        assertThat(corsResponse.getStatus(), is(OK.getStatusCode()));
        final String allowedHeaders = corsResponse.getHeaderString("access-control-allow-headers");
        assertThat(allowedHeaders, not(nullValue()));
        assertThat(asList(allowedHeaders.split(", ")), hasItems("CJSCPPUID", "CPPSID", "CPPCLIENTCORRELATIONID"));
    }
}
