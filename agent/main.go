package main

import (
	"log"
    "github.com/rowantrollope/redis-proxy/agent/agent"
    "github.com/rowantrollope/redis-proxy/agent/redisclient"
)

func main() {
    log.SetFlags(log.LstdFlags | log.Lshortfile)

    // Create an Agent instance
    agentInstance := agent.NewAgent()

    // Initialize RequestManager
    requestManager := agent.NewRequestManager()

    clientManager := redisclient.NewClientManager()

    // Start the HTTP server for activation requests
    go agent.StartHTTPServer(requestManager, clientManager, agentInstance)

    // Start handling messages from the server
    agentInstance.HandleServerMessages(requestManager, clientManager)

    // Start the heartbeat for existing Redis servers
    agentInstance.StartRedisServerHeartbeats(clientManager)

    // Inform the server about the Redis server IDs that this agent can manage
    for redisServerID := range clientManager.RedisServerDetailsMap {
        err := agentInstance.SendRedisServerID(redisServerID)
        if err != nil {
            log.Printf("Failed to inform server about redisServerID %s: %v", redisServerID, err)
        }
    }

    // Block forever
    select {}
}