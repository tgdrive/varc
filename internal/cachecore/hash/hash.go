package hash

// Type indicates a hash algorithm. Only None is required by the cache read path.
type Type int

// None indicates that no hashes are requested.
const None Type = 0

// Set indicates one or more hash types.
type Set int
