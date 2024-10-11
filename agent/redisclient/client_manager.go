package redisclient

import (
    "log"
    "sync"

    "github.com/rowantrollope/redis-proxy/agent/common"
)

type ClientManager struct {
    clients                  map[uint64]*RedisClient
    clientsMutex             sync.Mutex
    RedisServerDetailsMap    map[string]common.RedisServerDetails
    RedisServerDetailsMutex  sync.Mutex
    clientRedisServerIDMap   map[uint64]string
    clientRedisServerIDMutex sync.Mutex
}

func NewClientManager(config *common.ConfigManager) *ClientManager {
 
    return &ClientManager{
        clients:                make(map[uint64]*RedisClient),
        RedisServerDetailsMap:  config.GetServers(),
        clientRedisServerIDMap: make(map[uint64]string),
    }
}

func (cm *ClientManager) StoreClient(clientID uint64, client *RedisClient) {
    cm.clientsMutex.Lock()
    defer cm.clientsMutex.Unlock()
    cm.clients[clientID] = client
}
func (cm *ClientManager) GetClient(clientID uint64) (*RedisClient, bool) {
    cm.clientsMutex.Lock()
    defer cm.clientsMutex.Unlock()
    client, exists := cm.clients[clientID]
    return client, exists
}

func (cm *ClientManager) RemoveRedisServerDetails(redisServerID string) {
    cm.RedisServerDetailsMutex.Lock()
    defer cm.RedisServerDetailsMutex.Unlock()
	delete(cm.RedisServerDetailsMap, redisServerID)
}

func (cm *ClientManager) AssociateClientWithRedisServerID(clientID uint64, redisServerID string) {
    cm.clientRedisServerIDMutex.Lock()
    defer cm.clientRedisServerIDMutex.Unlock()
    cm.clientRedisServerIDMap[clientID] = redisServerID
}

func (cm *ClientManager) GetRedisServerDetails(redisServerID string) (common.RedisServerDetails, bool) {
    cm.RedisServerDetailsMutex.Lock()
    defer cm.RedisServerDetailsMutex.Unlock()
    details, exists := cm.RedisServerDetailsMap[redisServerID]
    return details, exists
}

func (cm *ClientManager) AddClient(clientID uint64, client *RedisClient) {
    cm.clientsMutex.Lock()
    defer cm.clientsMutex.Unlock()
    cm.clients[clientID] = client
}

func (cm *ClientManager) RemoveClient(clientID uint64) {
    cm.clientsMutex.Lock()
    defer cm.clientsMutex.Unlock()
    delete(cm.clients, clientID)
}

func (cm *ClientManager) HandleClientMessage(clientID uint64, data []byte) {
    cm.clientsMutex.Lock()
    client, exists := cm.clients[clientID]
    cm.clientsMutex.Unlock()
    if !exists {
        log.Printf("Received message for unknown ClientID %d", clientID)
        return
    }

    // Write the data to the client's Redis connection
	client.WriteToRedis(data)
}

func (cm *ClientManager) DisconnectClientsForRedisServerID(redisServerID string) {
	cm.clientsMutex.Lock()
	defer cm.clientsMutex.Unlock()
	for clientID, client := range cm.clients {
		cm.clientRedisServerIDMutex.Lock()
		clientRedisServerID, exists := cm.clientRedisServerIDMap[clientID]
		cm.clientRedisServerIDMutex.Unlock()

		if exists && clientRedisServerID == redisServerID {
			client.Cleanup()
		}
	}
}

func (cm *ClientManager) AddRedisServerDetails(uuid string, details common.RedisServerDetails) {
    cm.RedisServerDetailsMutex.Lock()
    defer cm.RedisServerDetailsMutex.Unlock()
    cm.RedisServerDetailsMap[uuid] = details
}
