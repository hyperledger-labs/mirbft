#! /bin/bash
#
# Generate a new SSH key and set it up for logging in to virtual server instances.
#
# ATTENTION: This key is also used to clone the Git repository on the cloud machine.
#            It must be thus (manually) added as a deploy key for that repository.

filename="ibmcloud-ssh-key"
key_name="ic-deploy-key"

ssh-keygen -t rsa -b 4096 -N "" -f "$filename"

key_id=$(ibmcloud sl security sshkey-add "$key_name" -f "$filename.pub" --output json | jq -r '.id')

for template_file in $(ls -1 cloud-machine-templates); do
  ibmcloud sl vs create --template "cloud-machine-templates/$template_file" --key "$key_id" --export "cloud-machine-templates/$template_file"

echo
echo "Generated SSH key:         $filename"
echo "IBM Cloud key ID:          $key_id"
echo "Set up with template file: $template_file"