package redisclient

// RedisServerDetails holds connection details for a Redis server
type RedisServerDetails struct {
    Host     string
    Port     string
    Username string
    Password string
}