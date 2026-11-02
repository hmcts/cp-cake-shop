#!/usr/bin/env bash

#The prerequisite for this script is that vagrant is running
#Script that runs, liquibase, deploys wars and runs integration tests

#context name is used to derive database name for running liquibase scripts and cake-shop uses framework database instead of it's own database
CONTEXT_NAME=cakeshop

FRAMEWORK_LIBRARIES_VERSION=$(mvn help:evaluate -Dexpression=framework-libraries.version -q -DforceStdout)
FRAMEWORK_VERSION=$(mvn help:evaluate -Dexpression=framework.version -q -DforceStdout)
EVENT_STORE_VERSION=$(mvn help:evaluate -Dexpression=event-store.version -q -DforceStdout)
FILE_SERVICE_VERSION=$(mvn help:evaluate -Dexpression=file-service.version -q -DforceStdout)

DOCKER_CONTAINER_REGISTRY_HOST_NAME=crmdvrepo01

LIQUIBASE_COMMAND=update
#LIQUIBASE_COMMAND=dropAll

#fail script on error
set -e

[ -z "$CPP_DOCKER_DIR" ] && echo "Please export CPP_DOCKER_DIR environment variable pointing to cpp-developers-docker repo (https://github.com/hmcts/cpp-developers-docker) checked out locally" && exit 1
WILDFLY_DEPLOYMENT_DIR="$CPP_DOCKER_DIR/containers/wildfly/deployments"

source $CPP_DOCKER_DIR/docker-utility-functions.sh
source $CPP_DOCKER_DIR/build-scripts/integration-test-scipt-functions.sh

source ${CPP_DOCKER_DIR}/build-scripts/download-liquibase-jar-functions.sh


runLiquibase() {
  runEventLogLiquibase
  runEventLogAggregateSnapshotLiquibase
  runEventBufferLiquibase
  runViewStoreLiquibase
  runSystemLiquibase
  runEventTrackingLiquibase
  runFileServiceLiquibase
  runJobStoreLiquibase
  printf "${CYAN}All liquibase $LIQUIBASE_COMMAND scripts run${NO_COLOUR}\n\n"
}

# cake-shop fetches its remote Features from WireMock (see WireMockFeatureFetcher), so stub the
# default Features as soon as WireMock is up; FeatureControlRefreshFailureIT changes this stub and restores it
stubDefaultFeatures() {
  local default_features_stub='{"request": {"method": "GET", "urlPath": "/cakeshop-feature-store/features"}, "response": {"status": 200, "jsonBody": {"features": [{"featureName": "recipes-have-allergens-specified", "enabled": false}]}}}'
  for attempt in {1..60}; do
    if curl -s -f -o /dev/null -X POST -H "Content-Type: application/json" -d "$default_features_stub" http://localhost:8080/__admin/mappings; then
      printf "${CYAN}Default cake-shop Features stubbed in WireMock${NO_COLOUR}\n"
      return 0
    fi
    sleep 2
  done
  echo "WireMock did not start; could not stub the default cake-shop Features"
  exit 1
}

buildAndDeploy() {
  loginToDockerContainerRegistry
  buildWars
  undeployWarsFromDocker
  buildAndStartContainers
  runLiquibase
  deployWiremock
  deployWars
  stubDefaultFeatures
  cakeshopDeploymentCheck
  integrationTests
}

buildAndDeploy
