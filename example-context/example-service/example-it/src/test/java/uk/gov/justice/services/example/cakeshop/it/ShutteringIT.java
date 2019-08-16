package uk.gov.justice.services.example.cakeshop.it;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.jsonassert.JsonAssert.with;
import static java.lang.Integer.parseInt;
import static java.lang.Integer.valueOf;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.slf4j.LoggerFactory.getLogger;
import static uk.gov.justice.services.jmx.api.state.ApplicationManagementState.SHUTTERED;
import static uk.gov.justice.services.jmx.api.state.ApplicationManagementState.UNSHUTTERED;
import static uk.gov.justice.services.jmx.system.command.client.connection.JmxParametersBuilder.jmxParameters;
import static uk.gov.justice.services.test.utils.common.host.TestHostProvider.getHost;
import static uk.gov.justice.services.test.utils.core.matchers.HttpStatusCodeMatcher.isStatus;

import uk.gov.justice.services.example.cakeshop.it.helpers.ApiResponse;
import uk.gov.justice.services.example.cakeshop.it.helpers.CommandSender;
import uk.gov.justice.services.example.cakeshop.it.helpers.EventFactory;
import uk.gov.justice.services.example.cakeshop.it.helpers.Querier;
import uk.gov.justice.services.example.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.jmx.api.command.ShutterSystemCommand;
import uk.gov.justice.services.jmx.api.command.UnshutterSystemCommand;
import uk.gov.justice.services.jmx.api.mbean.SystemCommanderMBean;
import uk.gov.justice.services.jmx.api.state.ApplicationManagementState;
import uk.gov.justice.services.jmx.system.command.client.SystemCommanderClient;
import uk.gov.justice.services.jmx.system.command.client.TestSystemCommanderClientFactory;
import uk.gov.justice.services.jmx.system.command.client.connection.JmxParameters;
import uk.gov.justice.services.test.utils.core.messaging.Poller;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import java.util.Optional;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response.Status;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

public class ShutteringIT {

    private static final Logger logger = getLogger(ShutteringIT.class);
    private static final String MARBLE_CAKE = "Marble cake";
    private static final String CARROT_CAKE = "Carrot cake";

    private final EventFactory eventFactory = new EventFactory();

    private Client client;
    private Querier querier;
    private CommandSender commandSender;

    private static final String HOST = getHost();
    private static final int PORT = parseInt(getProperty("random.management.port"));

    private final TestSystemCommanderClientFactory testSystemCommanderClientFactory = new TestSystemCommanderClientFactory();
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();
    private final Poller poller = new Poller();

    @Before
    public void before() {
        client = new RestEasyClientFactory().createResteasyClient();
        querier = new Querier(client);
        commandSender = new CommandSender(client, eventFactory);

        databaseCleaner.cleanSystemTables("framework");
        databaseCleaner.cleanEventStoreTables("framework");
    }

    @After
    public void cleanup() throws Exception {
        client.close();

        //invoke unshuttering - Always ensure unshutter is invoked as we cannot guarantee order of execution for other Cakeshop ITs

        final String contextName = "example-single";
        final JmxParameters jmxParameters = jmxParameters()
                .withHost(HOST)
                .withPort(PORT)
                .build();
        try (final SystemCommanderClient systemCommanderClient = testSystemCommanderClientFactory.create(jmxParameters)) {

            final SystemCommanderMBean systemCommanderMBean = systemCommanderClient.getRemote(contextName);

            systemCommanderMBean.call(new UnshutterSystemCommand());

            final Optional<ApplicationManagementState> applicationManagementState = poller.pollUntilFound(() -> {
                final ApplicationManagementState applicationState = systemCommanderMBean.getApplicationState();
                if (applicationState == UNSHUTTERED) {
                    return of(applicationState);
                }

                return empty();
            });

            if (! applicationManagementState.isPresent()) {
                fail();
            }
        }
    }

    @Test
    public void shouldNotReturnRecipesAfterShuttering() throws Exception {

        //invoke shuttering
        final String contextName = "example-single";
        final JmxParameters jmxParameters = jmxParameters()
                .withHost(HOST)
                .withPort(PORT)
                .build();
        try (final SystemCommanderClient systemCommanderClient = testSystemCommanderClientFactory.create(jmxParameters)) {
            final SystemCommanderMBean systemCommanderMBean = systemCommanderClient.getRemote(contextName);
            systemCommanderMBean.call(new ShutterSystemCommand());

            final Optional<ApplicationManagementState> applicationManagementState = poller.pollUntilFound(() -> {
                final ApplicationManagementState applicationState = systemCommanderMBean.getApplicationState();

                if (applicationState == SHUTTERED) {
                    return of(applicationState);
                }

                return empty();
            });

            if(! applicationManagementState.isPresent()) {
                fail();
            }
        }

        //add 2 recipes
        final String recipeId = addRecipe(MARBLE_CAKE);
        final String recipeId2 = addRecipe(CARROT_CAKE);

        Thread.sleep(5000L);

        //check recipes have not been added due to shuttering
        verifyRecipeAdded(recipeId, recipeId2, null, null, false, NOT_FOUND);
    }

    @Test
    public void shouldQueryForRecipesAfterUnShuttering() throws Exception {

        final String contextName = "example-single";
        final JmxParameters jmxParameters = jmxParameters()
                .withHost(HOST)
                .withPort(PORT)
                .build();
        try (final SystemCommanderClient systemCommanderClient = testSystemCommanderClientFactory.create(jmxParameters)) {

            final SystemCommanderMBean systemCommanderMBean = systemCommanderClient.getRemote(contextName);

            //invoke shuttering
            systemCommanderMBean.call(new ShutterSystemCommand());

            final Optional<ApplicationManagementState> applicationManagementState = poller.pollUntilFound(() -> {
                final ApplicationManagementState applicationState1 = systemCommanderMBean.getApplicationState();
                if (applicationState1 == SHUTTERED) {
                    return of(applicationState1);
                }

                return empty();
            });
            
            if(! applicationManagementState.isPresent()) {
                fail();
            }

            //add more recipes
            final String recipeId = addRecipe(MARBLE_CAKE);
            final String recipeId2 = addRecipe(CARROT_CAKE);

            Thread.sleep(5000L);

            //check recipes have not been added due to shuttering
            verifyRecipeAdded(recipeId, recipeId2, null, null, false, NOT_FOUND);

            //invoke unshuttering
            systemCommanderMBean.call(new UnshutterSystemCommand());

            final Optional<ApplicationManagementState> otherApplicationManagementState = poller.pollUntilFound(() -> {
                final ApplicationManagementState applicationState = systemCommanderMBean.getApplicationState();

                if (applicationState == UNSHUTTERED) {
                    return of(applicationState);
                }

                return empty();
            });

            if(! otherApplicationManagementState.isPresent()) {
                fail();
            }

            //check new recipes have been added successfully after unshuttering
            verifyRecipeAdded(recipeId, recipeId2, MARBLE_CAKE, CARROT_CAKE, true, OK);
        }
    }

    private void verifyRecipeAdded(final String recipeId,
                                   final String recipeId2,
                                   final String recipeName,
                                   final String recipeName2,
                                   final boolean checkRecipeName,
                                   final Status status) {
        final Optional<String> recId = of(recipeId);
        await().until(() -> {
            if (checkRecipeName) {
                final ApiResponse response = verifyResponse(empty(), status);

                verifyResponseBody(recipeId, recipeId2, recipeName, recipeName2, response);
            } else {
                verifyResponse(recId, status);
            }
        });
    }

    private void verifyResponseBody(final String recipeId, final String recipeId2, final String recipeName, final String recipeName2, final ApiResponse response) {
        with(response.body())
                .assertThat("$.recipes[?(@.id=='" + recipeId + "')].name", hasItem(recipeName))
                .assertThat("$.recipes[?(@.id=='" + recipeId2 + "')].name", hasItem(recipeName2));
    }

    private ApiResponse verifyResponse(final Optional<String> recipeId, final Status status) {
        final ApiResponse response = recipeId.isPresent() ? querier.queryForRecipe(recipeId.get()) :
                querier.recipesQueryResult();

        logger.info(format("Response: %s", response.httpCode()));
        assertThat(response.httpCode(), isStatus(status));

        return response;
    }


    private String addRecipe(final String cakeName) {
        final String recipeId = randomUUID().toString();
        commandSender.addRecipe(recipeId, cakeName);
        return recipeId;
    }
}
