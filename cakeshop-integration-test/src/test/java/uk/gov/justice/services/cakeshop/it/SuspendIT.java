package uk.gov.justice.services.cakeshop.it;

import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import uk.gov.justice.services.cakeshop.it.helpers.ApiResponse;
import uk.gov.justice.services.cakeshop.it.helpers.CommandSender;
import uk.gov.justice.services.cakeshop.it.helpers.EventFactory;
import uk.gov.justice.services.cakeshop.it.helpers.JmxParametersFactory;
import uk.gov.justice.services.cakeshop.it.helpers.Querier;
import uk.gov.justice.services.cakeshop.it.helpers.RestEasyClientFactory;
import uk.gov.justice.services.jmx.api.domain.SystemCommandStatus;
import uk.gov.justice.services.jmx.api.mbean.SystemCommanderMBean;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;
import uk.gov.justice.services.jmx.system.command.client.SystemCommanderClient;
import uk.gov.justice.services.jmx.system.command.client.TestSystemCommanderClientFactory;
import uk.gov.justice.services.test.utils.core.messaging.Poller;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;

import static com.jayway.jsonassert.JsonAssert.with;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.jupiter.api.Assertions.fail;
import static org.slf4j.LoggerFactory.getLogger;
import static uk.gov.justice.services.cakeshop.it.helpers.TestConstants.CONTEXT_NAME;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_COMPLETE;
import static uk.gov.justice.services.jmx.api.mbean.CommandRunMode.FORCED;
import static uk.gov.justice.services.jmx.api.mbean.CommandRunMode.GUARDED;
import static uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters.withNoCommandParameters;
import static uk.gov.justice.services.management.suspension.commands.SuspendCommand.SUSPEND;
import static uk.gov.justice.services.management.suspension.commands.UnsuspendCommand.UNSUSPEND;
import static uk.gov.justice.services.test.utils.core.matchers.HttpStatusCodeMatcher.isStatus;

public class SuspendIT {

    private static final Logger logger = getLogger(SuspendIT.class);
    private static final String MARBLE_CAKE = "Marble cake";
    private static final String CARROT_CAKE = "Carrot cake";

    private final EventFactory eventFactory = new EventFactory();

    private Client client;
    private Querier querier;
    private CommandSender commandSender;

    private final TestSystemCommanderClientFactory testSystemCommanderClientFactory = new TestSystemCommanderClientFactory();
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();
    private final Poller poller = new Poller();

    @BeforeEach
    public void before() {
        client = new RestEasyClientFactory().createResteasyClient();
        querier = new Querier(client);
        commandSender = new CommandSender(client, eventFactory);

        databaseCleaner.cleanSystemTables(CONTEXT_NAME);
        databaseCleaner.cleanEventStoreTables(CONTEXT_NAME);
        databaseCleaner.cleanViewStoreTables(CONTEXT_NAME,
                "cake",
                "cake_order",
                "recipe",
                "ingredient",
                "index",
                "index",
                "processed_event",
                "stream_buffer",
                "stream_status",
                "stream_error");
    }

    @AfterEach
    public void cleanup() throws Exception {
        client.close();

        //invoke unsuspending - Always ensure unsuspend is invoked as we cannot guarantee order of execution for other Cakeshop ITs
        try (final SystemCommanderClient systemCommanderClient = testSystemCommanderClientFactory.create(JmxParametersFactory.buildJmxParameters())) {

            final SystemCommanderMBean systemCommanderMBean = systemCommanderClient.getRemote(CONTEXT_NAME);

            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = withNoCommandParameters();
            final UUID commandId = systemCommanderMBean.call(
                    UNSUSPEND,
                    jmxCommandRuntimeParameters.getCommandRuntimeId(),
                    jmxCommandRuntimeParameters.getCommandRuntimeString(),
                    GUARDED.isGuarded()
            );

            final Optional<SystemCommandStatus> unsuspendStatus = poller.pollUntilFound(() -> {
                System.out.printf("Polling for command state to be COMMAND_COMPLETE for commandId: %s\n", commandId);
                final SystemCommandStatus commandStatus = systemCommanderMBean.getCommandStatus(commandId);
                if (commandStatus.getCommandState() == COMMAND_COMPLETE) {
                    return of(commandStatus);
                }

                return Optional.empty();
            });

            if (!unsuspendStatus.isPresent()) {
                fail();
            }
        }
    }

    @Test
    public void shouldNotReturnRecipesAfterSuspending() throws Exception {

        //invoke suspending
        try (final SystemCommanderClient systemCommanderClient = testSystemCommanderClientFactory.create(JmxParametersFactory.buildJmxParameters())) {
            final SystemCommanderMBean systemCommanderMBean = systemCommanderClient.getRemote(CONTEXT_NAME);
            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = withNoCommandParameters();
            final UUID commandId = systemCommanderMBean.call(
                    SUSPEND,
                    jmxCommandRuntimeParameters.getCommandRuntimeId(),
                    jmxCommandRuntimeParameters.getCommandRuntimeString(),
                    FORCED.isGuarded()
            );

            final Optional<SystemCommandStatus> suspendStatus = poller.pollUntilFound(() -> {
                System.out.printf("Polling for command state to be COMMAND_COMPLETE for commandId: %s\n", commandId);
                final SystemCommandStatus commandStatus = systemCommanderMBean.getCommandStatus(commandId);
                if (commandStatus.getCommandState() == COMMAND_COMPLETE) {
                    return of(commandStatus);
                }


                return Optional.empty();
            });

            if (!suspendStatus.isPresent()) {
                fail();
            }
        }

        //add 2 recipes
        final String recipeId = addRecipe(MARBLE_CAKE);
        final String recipeId2 = addRecipe(CARROT_CAKE);

        sleep(5000L);

        //check recipes have not been added due to suspending
        verifyRecipeAdded(recipeId, recipeId2, null, null, false, NOT_FOUND);
    }

    @Test
    public void shouldQueryForRecipesAfterUnShuttering() throws Exception {
        try (final SystemCommanderClient systemCommanderClient = testSystemCommanderClientFactory.create(JmxParametersFactory.buildJmxParameters())) {

            final SystemCommanderMBean systemCommanderMBean = systemCommanderClient.getRemote(CONTEXT_NAME);

            //invoke suspending
            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters = withNoCommandParameters();
            final UUID suspendCommandId = systemCommanderMBean.call(
                    SUSPEND,
                    jmxCommandRuntimeParameters.getCommandRuntimeId(),
                    jmxCommandRuntimeParameters.getCommandRuntimeString(),
                    FORCED.isGuarded()
            );

            final Optional<SystemCommandStatus> suspendStatus = poller.pollUntilFound(() -> {
                System.out.printf("Polling for command state to be COMMAND_COMPLETE for commandId: %s\n", suspendCommandId);
                final SystemCommandStatus commandStatus = systemCommanderMBean.getCommandStatus(suspendCommandId);
                if (commandStatus.getCommandState() == COMMAND_COMPLETE) {
                    return of(commandStatus);
                }


                return Optional.empty();
            });

            if (!suspendStatus.isPresent()) {
                fail();
            }

            //add more recipes
            final String recipeId = addRecipe(MARBLE_CAKE);
            final String recipeId2 = addRecipe(CARROT_CAKE);

            sleep(5000L);

            //check recipes have not been added due to suspending
            verifyRecipeAdded(recipeId, recipeId2, null, null, false, NOT_FOUND);

            //invoke unsuspending
            final UUID unsuspendCommandId = systemCommanderMBean.call(
                    UNSUSPEND,
                    jmxCommandRuntimeParameters.getCommandRuntimeId(),
                    jmxCommandRuntimeParameters.getCommandRuntimeString(),
                    FORCED.isGuarded());

            final Optional<SystemCommandStatus> unsuspendStatus = poller.pollUntilFound(() -> {
                System.out.printf("Polling for command state to be COMMAND_COMPLETE for commandId: %s\n", unsuspendCommandId);
                final SystemCommandStatus commandStatus = systemCommanderMBean.getCommandStatus(unsuspendCommandId);
                if (commandStatus.getCommandState() == COMMAND_COMPLETE) {
                    return of(commandStatus);
                }


                return Optional.empty();
            });

            if (!unsuspendStatus.isPresent()) {
                fail();
            }

            sleep(5000L);
            //check new recipes have been added successfully after unsuspending
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
                final ApiResponse response = verifyResponse(Optional.empty(), status);

                verifyResponseBody(recipeId, recipeId2, recipeName, recipeName2, response);
            } else {
                verifyResponse(recId, status);
            }

            return true;
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
