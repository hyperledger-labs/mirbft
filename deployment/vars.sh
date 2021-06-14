user="user"
group="group"
private_key_file="/path/to/ssh-key"

# Options to use when communicating with the remote machines.
ssh_options="-i $private_key_file -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60"