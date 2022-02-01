package discovery

const (
	PeerBasePort    = 10000
	WildcardAllTags = "__all__" // Used by the master only, and thus not part of the slave wildcard replacement.

	// These wildcards will be replaced in command arguments
	// and output file name by the corresponding values
	// when the slave executes the exec-start master command
	// (not to be confused by the command to execute).
	WildcardSlaveID   = "__id__"
	WildcardPublicIP  = "__public_ip__"
	WildcardPrivateIP = "__private_ip__"
)
