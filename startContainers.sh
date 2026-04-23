#!/usr/bin/env bash

##
#   Helper script to start docker containers
#
source ./runIntegrationTests.sh

startContainers() {
  loginToDockerContainerRegistry
  undeployWarsFromDocker
  buildAndStartContainers
  runLiquibase
}

startContainers
