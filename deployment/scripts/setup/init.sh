#! /bin/bash
#
# Initializes the IBM Cloud CLI.
# Logs in and, if no ssh key is present on the local machine,
# Generates one and sets it up with IBM Cloud.

ic_api_key_file=ibmcloud-api-key
ssh_key_file="ibmcloud-ssh-key"

function complain_about_missing_key() {
    >&2 echo "No IBM Cloud API key file found.
Save your key in a file called ${ic_api_key_file} in the current directory and retry.
If you do not have an API key, create one by logging in through

    ibmcloud login

using using your IBM Cloud user name and password (or some other login method,
most likely a one-time password, in which case the command to run is
ibmcloud) and executing

    ibmcloud iam api-key-create MyKey -d \"this is my API key\" --file ${ic_api_key_file}

while replacing MyKey and \"this is my API key\" by your key's name and description.
For more documentation, see https://cloud.ibm.com/docs/iam?topic=iam-userapikey"
}

if [ -r $ic_api_key_file ]; then
  ibmcloud login --apikey "@$ic_api_key_file"
else
  complain_about_missing_key
fi

# If there is no ssh key, set up a new one.
if [ ! -e "$ssh_key_file" ]; then
  scripts/setup/add-ssh-key.sh
fi
