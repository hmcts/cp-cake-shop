#!/usr/bin/env bash

#The prerequisite for this script is that vagrant is running
#Script that runs, liquibase, deploys wars and runs integration tests

#context name is used to derive database name for running liquibase scripts and cake-shop uses framework database instead of it's own database
CONTEXT_NAME=framework

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
  runViewStoreLiquibaseForCakeShopContext
  runSystemLiquibase
  runEventTrackingLiquibase
  runFileServiceLiquibase
  runJobStoreLiquibase
  printf "${CYAN}All liquibase $LIQUIBASE_COMMAND scripts run${NO_COLOUR}\n\n"
}

buildAndDeploy() {
  #Unlike other contexts, this script doesn't provide capability to run integration tests, main intention of this script is to just deploy cakeshop to local dev environment so that ITs can be executed froM Intellij/IDE for debugging purpose.
  loginToDockerContainerRegistry
  #buildWarsForCakeShopContext #This is not going to run tests, i.e. builds using -DskipTests
  undeployWarsFromDocker
  #buildAndStartContainers
  #runLiquibase
  deployWiremock
  deployWarsForCakeShopContext
  contextHealthchecksForCakeShop
}

buildAndDeploy
