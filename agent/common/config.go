package common

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/google/uuid"
)

const ConfigFile = "agent_config.json"

const DEFAULT_PROXY_SERVER_URL = "ws://127.0.0.1:8080/agent"

// RedisServerDetails holds connection details for a Redis server
type RedisServerDetails struct {
    Host     string
    Port     string
    Username string
    Password string
}

// AgentConfig represents the agent's configuration.
type AgentConfig struct {
	AgentID string                          `json:"agent_id"`
	Servers map[string]RedisServerDetails   `json:"servers"`
	ProxyServerURL string                      `json:"proxy_server"`
	// Add other fields as needed
}

// ConfigManager handles concurrent access to AgentConfig.
type ConfigManager struct {
	mu     sync.RWMutex
	config *AgentConfig
}
// NewConfigManager creates a new ConfigManager, loading the configuration from the file or creating a new one.
func NewConfigManager() (*ConfigManager, error) {
	var config AgentConfig

	if _, err := os.Stat(ConfigFile); err == nil {
		// Configuration file exists, read it
		data, err := os.ReadFile(ConfigFile)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(data, &config)
		if err != nil {
			return nil, err
		}
	} else {
		// Configuration file does not exist, create a new one
		config = AgentConfig{
			AgentID: uuid.New().String(),
			Servers: make(map[string]RedisServerDetails),
			ProxyServerURL: DEFAULT_PROXY_SERVER_URL,
			// Initialize other fields if needed
		}
		// Save the new configuration
		data, err := json.MarshalIndent(config, "", "  ")
		if err != nil {
			return nil, err
		}
		err = os.WriteFile(ConfigFile, data, 0600)
		if err != nil {
			return nil, err
		}
	}

	return &ConfigManager{
		config: &config,
	}, nil
}

// Save writes the current configuration to the configuration file.
func (cm *ConfigManager) save() error {

	data, err := json.MarshalIndent(cm.config, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(ConfigFile, data, 0600)
}

func (cm *ConfigManager) GetAgentID() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.config.AgentID
}
func (cm *ConfigManager) GetProxyServerURL() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.config.ProxyServerURL
}
func (cm *ConfigManager) GetServers() map[string]RedisServerDetails {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Create a copy to prevent concurrent map access
	serversCopy := make(map[string]RedisServerDetails)
	for k, v := range cm.config.Servers {
		serversCopy[k] = v
	}
	return serversCopy
}

func (cm *ConfigManager) AddServer(serverID string, details RedisServerDetails) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.config.Servers[serverID] = details
	return cm.save()
}

func (cm *ConfigManager) RemoveServer(serverID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.config.Servers, serverID)
	return cm.save()
}