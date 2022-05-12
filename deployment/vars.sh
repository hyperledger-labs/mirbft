# This file should not be an executable, it only holds variables
# SPECIFY BELOW

# SSH user for access to remote machines, no need to specify for the local deployment.
ssh_user="user"

# Key for SSH access to remote machines, no need to specify it for a local deployment
# All machines must be accessible with the same SSH key
private_key_file="/path/to/ssh-key"

#=======================================================================================================================

#DO NOT EDIT BELOW

# Options to use when communicating with the remote machines.
ssh_options="-i $private_key_file -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60"