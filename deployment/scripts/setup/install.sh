#! /bin/bash

# Installs the packages required to use the IBM Cloud CLI.
# Only works on Ubuntu Linux.

echo "Updating packages."
sudo apt-get update

echo "Installing Docker and curl (required by IBM Cloud CLI)."
sudo apt-get install docker docker-compose curl

echo "Installing jq (for parsing CLI command output)."
sudo apt-get install jq

echo "Installing IBM Cloud CLI using official installation script."
curl -sL https://ibm.biz/idt-installer | bash

echo "This should should list the usage instructions, the current version, and the supported commands."
ibmcloud dev help

echo "Done."
