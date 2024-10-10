package redisclient

import (
    "os"
    "log"
    "sync"
    "encoding/json"
)

type ClientManager struct {
    clients                  map[uint64]*RedisClient
    clientsMutex             sync.Mutex
    RedisServerDetailsMap    map[string]RedisServerDetails
    RedisServerDetailsMutex  sync.Mutex
    clientRedisServerIDMap   map[uint64]string
    clientRedisServerIDMutex sync.Mutex
}

func NewClientManager() *ClientManager {

    // Load Redis server details
    redisDetailsMap, err := loadRedisServerDetails()
    if err != nil {
        log.Fatalf("Failed to load Redis server details: %v", err)
    }
        
    return &ClientManager{
        clients:                make(map[uint64]*RedisClient),
        RedisServerDetailsMap:  redisDetailsMap,
        clientRedisServerIDMap: make(map[uint64]string),
    }
}

func loadRedisServerDetails() (map[string]RedisServerDetails, error) {
    const fileName = "redis_servers.json"
    data, err := os.ReadFile(fileName)
    if err != nil {
        if os.IsNotExist(err) {
            return make(map[string]RedisServerDetails), nil
        }
        return nil, err
    }
    var detailsMap map[string]RedisServerDetails
    err = json.Unmarshal(data, &detailsMap)
    if err != nil {
        return nil, err
    }
    return detailsMap, nil
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

func (cm *ClientManager) SaveRedisServerDetails(detailsMap map[string]RedisServerDetails) error {
    const fileName = "redis_servers.json"
    data, err := json.MarshalIndent(detailsMap, "", "  ")
    if err != nil {
        return err
    }
    return os.WriteFile(fileName, data, 0600)
}

func (cm *ClientManager) GetRedisServerDetails(redisServerID string) (RedisServerDetails, bool) {
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

func (cm *ClientManager) AddRedisServerDetails(uuid string, details RedisServerDetails) {
    cm.RedisServerDetailsMutex.Lock()
    defer cm.RedisServerDetailsMutex.Unlock()
    cm.RedisServerDetailsMap[uuid] = details
}
