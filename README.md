[![Coverage Status](https://coveralls.io/repos/github/hmcts/cp-cake-shop/badge.svg?branch=main)](https://coveralls.io/github/hmcts/cp-cake-shop?branch=main)


# cake-shop

## How deployment works?
Unlike other contexts, this cakeshop project uses a different approach as this belong to framework (github). Integration tests of this component gets executed in CI pipeline which runs
in travis. So, to get this working in travis pipeline this component gets deployed to embedded wildfly using maven plugin and then ITs are executed against embedded wildfly instance.
All this is setup through integration-test module's pom.xml. Artemis runs as embedded broker in wildfly. Spinning up embedded wildfly uses random ports approach i.e. free ports are selected by maven plugin
and then those ports are used to spin up required services and this is necessary to avoid port clash with local dev environment, so that there is no requirement to shutdown local dev environment
before running build on this component. There is a separate version of standalone.xml file in integration-test module that is used for embedded wildfly instance

By running `mvn clean install` basically it builds the project and deploys service war file to embedded wildfly instance and then runs all Integration tests. While running 

One issue with above approach is, it's bit hard to debug any integration tests from IDE (it's not impossible but requires little bit of tweaks to setup process). In order to support
debugging ITs this application can also be deployed to local dev environment using this [script](runIntegrationTests.sh). This script builds application without any tests and deploys service war file to
local dev environment. Until other contexts this script is not going to run any integration tests. Once application is deployed any IT can be run from intellij as normal.

Port selection switching logic is incorporated in on the test helper class, so that when a test is run from IDE it returns local dev environment ports, but when tests are run through mvn command
it returns random ports that are selected by maven plugin for running the application using embedded wildfly.


