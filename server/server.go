package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"encoding/binary"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

// Define the possible client disconnect methods
type ClientDisconnectMethod string

const (
	DISCONNECT_AUTO    ClientDisconnectMethod = "AUTO"
	DISCONNECT_TIMEOUT ClientDisconnectMethod = "TIMEOUT"
	DISCONNECT_NONE    ClientDisconnectMethod = "NONE"
)

type Server struct {
	agentUpgrader websocket.Upgrader

	agents sync.Map // Concurrent safe map[string]*Agent

	basePort int
	port     int

	rdb *redis.Client   // Redis client for database
	ctx context.Context // Context for Redis operations

	// Add the following fields
	listenerPerRedisServerID      map[string]net.Listener
	listenerPerRedisServerIDMutex sync.RWMutex

	// Client disconnect method
	clientDisconnectMethod ClientDisconnectMethod

	// Active clients counter
	activeClients      map[string]int
	activeClientsMutex sync.Mutex
}

type RedisServerInfo struct {
	AccountID    string `json:"account_id"`
	FriendlyName string `json:"friendly_name"`
	Status       string `json:"status"`
	Timestamp    int64  `json:"timestamp"`
}

func NewServer(disconnectMethod ClientDisconnectMethod) *Server {
	return &Server{
		agentUpgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		agents:                   sync.Map{},
		basePort:                 6400,
		port:                     8080,
		ctx:                      context.Background(),
		listenerPerRedisServerID: make(map[string]net.Listener),
		clientDisconnectMethod:   disconnectMethod,
		activeClients:            make(map[string]int),
	}
}

// Agent represents a connected agent with its clients
type Agent struct {
	conn       *websocket.Conn
	clients    map[uint64]*Client // Map ClientID to Client
	clientsMux sync.RWMutex       // Mutex for clients map
	register   chan *Client
	unregister chan *Client
	messages   chan Message // Channel for incoming messages with ClientID
	done       chan struct{}
	pingPeriod time.Duration
	pongWait   time.Duration
	send       chan Message
	agentID    string  // Add agentID field
	server     *Server // Add reference to Server

	connected bool
	mu        sync.Mutex
}

// Client represents a connected client
type Client struct {
	ID             uint64
	conn           net.Conn
	send           chan []byte
	lastActive     time.Time
	RedisServerID  string    // Add this field
	disconnectOnce sync.Once // Add this field
	disconnectChan chan struct{}
}

type Message struct {
	ClientID    uint64
	Data        []byte
	MessageType string // "control" or "data" (optional)
}

var clientIDCounter uint64
var clientIDMutex = &sync.Mutex{}

func generateUniqueClientID() uint64 {
	clientIDMutex.Lock()
	defer clientIDMutex.Unlock()
	clientIDCounter++
	return clientIDCounter
}

func main() {
	// Configure the logger to include timestamps and file line numbers
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	server := NewServer(DISCONNECT_NONE)

	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found or error loading .env file, proceeding to use system environment variables")
	}

	// Read Redis configuration from environment variables
	redisAddr := os.Getenv("REDIS_ADDRESS")
	redisPassword := os.Getenv("REDIS_PASSWORD")

	// Check if Redis address is set
	if redisAddr == "" {
		log.Fatal("REDIS_ADDRESS environment variable is not set")
	}

	// Initialize Redis client
	server.rdb = redis.NewClient(&redis.Options{
		Addr:     redisAddr, // Adjust as needed
		Password: redisPassword,
	})

	// Test Redis connection
	_, err = server.rdb.Ping(server.ctx).Result()

	if err != nil {
		log.Fatal("Error connecting to Redis:", err)
	}

	log.Println("Connected to Redis")

	// Create a new ServeMux
	mux := http.NewServeMux()

	// Register your handlers
	mux.HandleFunc("/agent", server.handleAgentConnection)
	mux.HandleFunc("/servers", server.handleGetServers)
	mux.HandleFunc("/stats", server.handleGetStats)
	mux.HandleFunc("/connect", server.handleConnectToRedisServer)
	mux.HandleFunc("/disconnect", server.handleDisconnectFromRedisServer)
	mux.HandleFunc("/activation/claim", server.handleActivationClaim)

	// Wrap the mux with the CORS middleware
	handler := enableCors(mux)

	// Start the HTTP server with the handler
	log.Printf("Server listening on :%d for agents and API requests", server.port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", server.port), handler); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// Handle activation claim requests from clients (management console)
func (s *Server) handleActivationClaim(w http.ResponseWriter, r *http.Request) {
	activationCode := r.URL.Query().Get("activationCode")
	accountID := r.URL.Query().Get("accountID")
	friendlyName := r.URL.Query().Get("friendlyName") // Friendly name parameter

	log.Println("handleActivationClaim activation code: " + activationCode + " accountID: " + accountID + " friendlyName: " + friendlyName)

	if activationCode == "" || accountID == "" {
		http.Error(w, "Missing activationCode or accountID parameter", http.StatusBadRequest)
		return
	}

	// Check if the activation code is pending
	redisServerID, err := s.rdb.Get(s.ctx, "pendingActivation:"+activationCode).Result()
	if err == redis.Nil {
		// Activation code not found or already claimed
		http.Error(w, "Invalid or already claimed activation code", http.StatusNotFound)
		return
	} else if err != nil {
		log.Println("Error checking pending activation:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Remove the pending activation
	err = s.rdb.Del(s.ctx, "pendingActivation:"+activationCode).Err()
	if err != nil {
		log.Println("Error deleting pending activation:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Remove the activation code associated with the UUID
	err = s.rdb.Del(s.ctx, "redis_server_id:"+redisServerID+":activation_code").Err()
	if err != nil {
		log.Println("Error deleting activation code for redis_server_id:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Create the ServerInfo object
	redisServerInfo := RedisServerInfo{
		AccountID:    accountID,
		FriendlyName: friendlyName,
		Status:       "RUNNING",
		Timestamp:    time.Now().Unix(),
	}

	// Store the redisServerInfo as a JSON object in Redis
	redisServerInfoJSON, err := json.Marshal(redisServerInfo)
	if err != nil {
		log.Println("Error marshaling server info:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Use JSON.SET command to store the JSON object
	_, err = s.rdb.Do(s.ctx, "JSON.SET", "redis_server_id:"+redisServerID, ".", redisServerInfoJSON).Result()
	if err != nil {
		log.Println("Error setting server info in Redis:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Add UUID to the set of servers for the accountID
	err = s.rdb.SAdd(s.ctx, "accountServers:"+accountID, redisServerID).Err()
	if err != nil {
		log.Println("Error adding UUID to accountServers:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Respond with success
	log.Println("handleActivationClaim success")
	response := map[string]string{
		"status":          "activation code claimed successfully",
		"redis_server_id": redisServerID,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Store mapping from agentID to redisServerID
func (s *Server) associateAgentWithRedisServerID(agentID, redisServerID string) error {
	key := "agentIDToRedisServerIDs:" + agentID
	err := s.rdb.SAdd(s.ctx, key, redisServerID).Err()
	if err != nil {
		return err
	}
	// Also store reverse mapping
	return s.rdb.Set(s.ctx, "redisServerIDToAgentID:"+redisServerID, agentID, 0).Err()
}

// Get all redisServerIDs associated with an agentID
func (s *Server) getRedisServerIDsForAgent(agentID string) ([]string, error) {
	key := "agentIDToRedisServerIDs:" + agentID
	return s.rdb.SMembers(s.ctx, key).Result()
}

// Get serverID for  redisServerIDs associated with an agentID
func (s *Server) getAgentIDForRedisServerID(redisServerID string) (string, error) {
	key := "redisServerIDToAgentID:" + redisServerID
	return s.rdb.Get(s.ctx, key).Result()
}

// Handle retrieval of servers associated with an accountID
func (s *Server) handleGetServers(w http.ResponseWriter, r *http.Request) {
	accountID := r.URL.Query().Get("accountID")
	if accountID == "" {
		http.Error(w, "Missing accountID parameter", http.StatusBadRequest)
		return
	}

	// Get the set of UUIDs associated with the accountID
	uuids, err := s.rdb.SMembers(s.ctx, "accountServers:"+accountID).Result()
	if err != nil {
		log.Println("Error getting servers for accountID:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Prepare a slice to hold server info
	servers := []map[string]string{}

	// For each UUID, retrieve the serverInfo JSON object
	for _, uuid := range uuids {
		// Get the JSON data from Redis
		serverInfoData, err := s.rdb.Do(s.ctx, "JSON.GET", "uuid:"+uuid).Result()
		if err == redis.Nil {
			// Server info not found
			log.Println("Server info not found for UUID:", uuid)
			continue
		} else if err != nil {
			log.Println("Error getting server info for UUID:", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Parse the JSON data
		var serverInfo RedisServerInfo
		switch data := serverInfoData.(type) {
		case string:
			err = json.Unmarshal([]byte(data), &serverInfo)
		case []byte:
			err = json.Unmarshal(data, &serverInfo)
		default:
			log.Println("Unknown data type for server info:", reflect.TypeOf(serverInfoData))
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		if err != nil {
			log.Println("Error unmarshaling server info for UUID:", uuid, err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Append server info to the list
		servers = append(servers, map[string]string{
			"uuid":         uuid,
			"friendlyName": serverInfo.FriendlyName,
		})
	}

	response := map[string][]map[string]string{
		"servers": servers,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Handle retrieval of servers associated with a databaseID
func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	redisServerId := r.URL.Query().Get("id")
	if redisServerId == "" {
		http.Error(w, "Missing redisServerId parameter", http.StatusBadRequest)
		return
	}

	// Get the stats for the redisServerId
	stats, err := s.rdb.Do(s.ctx, "JSON.GET", "redis_server_id:"+redisServerId+":heartbeat_latest").Result()
	if err != nil {
		log.Println("Error getting stats for redisServerId: %s, Error: %v", redisServerId, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// return stats to client
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleAgentConnection handles WebSocket connections from agents
func (s *Server) handleAgentConnection(w http.ResponseWriter, r *http.Request) {
	log.Println("Received a new agent connection request")
	// Upgrade HTTP to WebSocket
	wsConn, err := s.agentUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade to WebSocket: %v", err)
		http.Error(w, fmt.Sprintf("Failed to upgrade to WebSocket: %v", err), http.StatusBadRequest)
		return
	}

	// Read the agent ID sent by the agent
	log.Println("Waiting to receive agent ID")
	_, agentIDBytes, err := wsConn.ReadMessage()
	if err != nil {
		log.Printf("Failed to read agent ID: %v", err)
		http.Error(w, fmt.Sprintf("Failed to read agent ID: %v", err), http.StatusBadRequest)
		wsConn.Close()
		return
	}
	agentID := string(agentIDBytes)
	log.Printf("Agent connected: %s", agentID)

	agent := &Agent{
		conn:       wsConn,
		clients:    make(map[uint64]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		messages:   make(chan Message),
		done:       make(chan struct{}),
		pingPeriod: 54 * time.Second,
		pongWait:   60 * time.Second,
		send:       make(chan Message, 100), // Make agent.send a buffered channel
		agentID:    agentID,
		server:     s,
		connected:  true, // Set connected to true
	}

	// Store the agent
	s.StoreAgent(agentID, agent)

	// Retrieve redisServerIDs associated with this agentID from Redis
	redisServerIDs, err := s.getRedisServerIDsForAgent(agentID)
	if err != nil {
		log.Printf("Error retrieving redisServerIDs for agentID %s: %v", agentID, err)
	}

	log.Printf("redisServerIDs for agentID %s: %v", agentID, redisServerIDs)

	// Start agent's run loop
	go agent.run()

	// Start read and write pumps
	go agent.readPump()
	go agent.writePump()

	// Set up ping/pong handlers to detect disconnections
	agent.setupPingPong()

	// Optionally, send periodic pings
	go agent.startPing()
}

// agent.run handles registration and broadcasting to clients
func (agent *Agent) run() {
	for {
		select {
		case message := <-agent.messages:
			// Route the message to the correct client
			agent.clientsMux.RLock()
			client, ok := agent.clients[message.ClientID]
			agent.clientsMux.RUnlock()
			if ok {
				client.send <- message.Data
			} else {
				log.Printf("Client %d not found for agent %s", message.ClientID, agent.agentID)
			}
		}
	}
}

// agent.readPump reads from wsConn and broadcasts to clients
func (agent *Agent) readPump() {
	defer func() {
		agent.conn.Close()
	}()
	for {
		_, reader, err := agent.conn.NextReader()
		if err != nil {
			log.Printf("Error reading from WebSocket: %v", err)
			break
		}

		for {
			message, err := readMessage(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading message: %v", err)
				break
			}
			if message.ClientID == 0 {
				// Control message
				err := agent.handleControlMessage(message.Data)
				if err != nil {
					log.Printf("Error handling control message: %v", err)
				}
			} else {
				// Data message
				// Send the message to the appropriate client
				agent.messages <- message
			}
		}
	}
}

func (agent *Agent) handleControlMessage(data []byte) error {
	// Parse the JSON data
	var controlMsg map[string]interface{}
	err := json.Unmarshal(data, &controlMsg)
	if err != nil {
		return fmt.Errorf("failed to parse control message JSON: %v", err)
	}

	// Extract the message type
	msgType, ok := controlMsg["message_type"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'message_type' field")
	}

	switch msgType {
	case "__activation__":
		// Handle activation
		return agent.server.handleActivation(agent.agentID, controlMsg)
	case "__agent_managed_redis_server__":
		return agent.server.handleAgentManagedRedisServer(controlMsg, agent.agentID)
	case "__redis_server_heartbeat__":
		return agent.server.handleRedisServerHeartbeat(controlMsg, agent.agentID)
	default:
		return fmt.Errorf("unknown control message type: %s", msgType)
	}
}

func (s *Server) handleRedisServerHeartbeat(controlMsg map[string]interface{}, agentID string) error {
	redisServerID, ok := controlMsg["redis_server_id"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'redis_server_id' field")
	}

	status, ok := controlMsg["status"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'status' field")
	}

	statsJSON, ok := controlMsg["redis_stats"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'redis_stats' field")

	}

	// Unmarshal the JSON string into a map
	var stats map[string]interface{}
	if err := json.Unmarshal([]byte(statsJSON), &stats); err != nil {
		log.Printf("Failed to unmarshal redis_stats JSON: %v", err)
		return err
	}

	// Flatten the stats map to string fields
	flatStats := make(map[string]interface{})
	for k, v := range stats {
		flatStats[k] = fmt.Sprint(v)
	}

	// Add status and timestamp to the stats
	flatStats["status"] = status
	flatStats["timestamp"] = time.Now().Unix()

	// Store in Redis Stream
	streamKey := fmt.Sprintf("redis_server_id:%s:heartbeat_stream", redisServerID)
	_, err := s.rdb.XAdd(s.ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: 1000,
		Approx: true,
		Values: flatStats,
	}).Result()
	if err != nil {
		log.Printf("Error adding heartbeat to stream: %v", err)
		return err
	}

	// Store the status and timestamp to the serverInfo 
	serverInfoKey := fmt.Sprintf("redis_server_id:%s", redisServerID) 

	_, err = s.rdb.Do(s.ctx, "JSON.SET", serverInfoKey, "$.timestamp", time.Now().Unix()).Result()
	if err != nil {
		log.Printf("Error storing timestamp to serverInfo JSON: %v", err)
		return err
	}
	statusString := fmt.Sprintf("\"%s\"", status)
	_, err = s.rdb.Do(s.ctx, "JSON.SET", serverInfoKey, "$.status", statusString).Result()
	if err != nil {
		log.Printf("Error storing status to serverInfo JSON: %v", err)
		return err
	}
	
	// Store the latest stats as JSON for quick access
	jsonKey := fmt.Sprintf("redis_server_id:%s:heartbeat_latest", redisServerID)
	stats["timestamp"] = time.Now().Unix()
	statsJSONBytes, err := json.Marshal(stats)
	if err != nil {
		log.Printf("Error marshaling latest heartbeat stats: %v", err)
		return err
	}

	// Use JSON.SET command to store the JSON object
	_, err = s.rdb.Do(s.ctx, "JSON.SET", jsonKey, ".", statsJSONBytes).Result()
	if err != nil {
		log.Printf("Error storing latest heartbeat stats as JSON: %v", err)
		return err
	}

	log.Printf("Heartbeat for redisServerID %s - Server is %s", redisServerID, status)
	return nil
}

func (s *Server) handleAgentManagedRedisServer(controlMsg map[string]interface{}, agentID string) error {
	redisServerID, ok := controlMsg["redis_server_id"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'redis_server_id' field")
	}

	// Associate the agentID with the redisServerID in Redis
	return s.associateAgentWithRedisServerID(agentID, redisServerID)
}

func (s *Server) handleActivation(agentID string, controlMsg map[string]interface{}) error {
	// Extract the UUID of the activating client
	redisServerID, ok := controlMsg["redis_server_id"].(string)
	if !ok {
		return fmt.Errorf("activation message missing 'redis_server_uuid' field")
	}

	// Extract `request_id` from the control message
	requestID, ok := controlMsg["request_id"].(string)
	if !ok {
		return fmt.Errorf("activation message missing 'request_id' field")
	}

	// Implement the activation logic here
	log.Printf("Activating agent %s for redis server ID: %s", agentID, redisServerID)

	// Perform any necessary activation logic
	// Check if the UUID is already registered (claimed)
	serverInfoData, err := s.rdb.Do(s.ctx, "JSON.GET", "redis_server_id:"+redisServerID).Result()
	if err == redis.Nil {
		// UUID not registered
		log.Println("UUID not registered:", redisServerID)

		// Check for existing activation code
		activationCode, err := s.rdb.Get(s.ctx, "redis_server_id:"+redisServerID+":activation_code").Result()
		if err == redis.Nil {
			// No activation code exists, generate and store one
			activationCode, err := s.generateActivationCode()
			if err != nil {
				log.Println("Error generating activation code:", err)
				return err
			}

			// Store the activation code associated with the UUID
			err = s.rdb.Set(s.ctx, "redis_server_id:"+redisServerID+":activation_code", activationCode, 24*time.Hour).Err()
			if err != nil {
				log.Println("Error storing activation code for redis_server_id:", err)
				return err
			}

			// Map the activation code to the UUID for pending activation
			err = s.rdb.Set(s.ctx, "pendingActivation:"+activationCode, redisServerID, 24*time.Hour).Err()
			if err != nil {
				log.Println("Error storing pending activation:", err)
				return err
			}

			// Send activation code back to agent
			responseData := map[string]string{
				"activation_code": activationCode,
			}
			return s.sendActivationResponse(agentID, requestID, responseData)
		} else if err != nil {
			log.Println("Error checking activation code for redis_server_id:", err)
			return err
		} else {
			// After activation is successful, store the mapping
			// Activation code exists, send it back to the agent
			responseData := map[string]string{
				"activation_code": activationCode,
			}
			return s.sendActivationResponse(agentID, requestID, responseData)
		}
	} else if err != nil {
		log.Println("Error retrieving server info for redis_server_id:", err)
		return err
	} else {
		// UUID is registered, parse the ServerInfo
		var serverInfo RedisServerInfo
		switch data := serverInfoData.(type) {
		case string:
			err = json.Unmarshal([]byte(data), &serverInfo)
		case []byte:
			err = json.Unmarshal(data, &serverInfo)
		default:
			log.Println("Unknown data type for server info:", reflect.TypeOf(serverInfoData))
			return fmt.Errorf("unknown data type for server info")
		}

		if err != nil {
			log.Println("Error unmarshaling server info for redis_server_id:", redisServerID, err)
			return err
		}

		// Send activated response
		log.Printf("Account ID found for redis_server_id: %s: %s", redisServerID, serverInfo.AccountID)
		responseData := map[string]bool{
			"activated": true,
		}

		return s.sendActivationResponse(agentID, requestID, responseData)
	}

}

func (s *Server) sendActivationResponse(agentID, requestID string, responseData interface{}) error {
	agent, ok := s.LoadAgent(agentID)
	if !ok {
		return fmt.Errorf("agent %s not found", agentID)
	}

	controlResp := map[string]interface{}{
		"message_type": "__activation_response__",
		"request_id":   requestID,
		"status":       "success",
		"data":         responseData,
	}

	respData, err := json.Marshal(controlResp)
	if err != nil {
		return fmt.Errorf("failed to marshal activation response: %v", err)
	}

	message := Message{
		ClientID: 0,
		Data:     respData,
	}

	agent.send <- message
	return nil
}

// generateActivationCode generates a cryptographically secure activation code
// in the format XXXX-XXXX, where each X is an uppercase alphanumeric character.
func (server *Server) generateActivationCode() (string, error) {
	// I and O are removed to avoid confusion with 1 and 0
	const charset = "ABCDEFGHJKLMNPQRSTUVWXYZ0123456789"
	code := make([]byte, 8) // 8 characters (without the dash)

	for i := range code {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", err
		}
		code[i] = charset[num.Int64()]
	}

	// Insert the dash in the middle
	activationCode := fmt.Sprintf("%s-%s", string(code[:4]), string(code[4:]))
	return activationCode, nil
}

func (agent *Agent) unregisterClientByID(clientID uint64) {
	agent.unregister <- &Client{ID: clientID}
}

func (s *Server) sendControlMessage(agentID string, controlData map[string]interface{}) error {
	agent, ok := s.LoadAgent(agentID)
	if !ok {
		return fmt.Errorf("agent %s not found", agentID)
	}

	// Serialize the control data to JSON
	jsonData, err := json.Marshal(controlData)
	if err != nil {
		return fmt.Errorf("failed to marshal control data: %v", err)
	}

	// Create a Message with ClientID == 0
	message := Message{
		ClientID: 0,
		Data:     jsonData,
	}

	// Send the control message to the agent
	agent.send <- message

	return nil
}

// handleConnectToRedisServer handles the API request to connect to an agent /Connect
func (s *Server) handleConnectToRedisServer(w http.ResponseWriter, r *http.Request) {
	redisServerID := r.URL.Query().Get("redis_server_id")
	log.Printf("Received /connect request for redis_server_id: %s", redisServerID)
	if redisServerID == "" {
		http.Error(w, "redis_server_id is required", http.StatusBadRequest)
		return
	}

	agentID, err := s.getAgentIDForRedisServerID(redisServerID)

	if err != nil {
		log.Printf("No agent found for redis_server_id %s", redisServerID)
		http.Error(w, "Agent not found for the provided redis_server_id", http.StatusNotFound)
		return
	}

	// Check if the agent is connected
	_, ok := s.LoadAgent(agentID)
	if !ok {
		log.Printf("Agent %s not found", agentID)
		http.Error(w, "Agent not found", http.StatusNotFound)
		return
	}

	// Retrieve the existing listener for this redisServerID
	s.listenerPerRedisServerIDMutex.RLock()
	listener, exists := s.listenerPerRedisServerID[redisServerID]
	s.listenerPerRedisServerIDMutex.RUnlock()

	if !exists {
		// Create a new listener for the redisServerID
		listener = s.createListenerForRedisServerID(redisServerID)
		if listener == nil {
			log.Printf("Failed to create listener for redis_server_id %s", redisServerID)
			http.Error(w, "Failed to create listener", http.StatusInternalServerError)
			return
		}

		// Store the listener
		s.listenerPerRedisServerIDMutex.Lock()
		s.listenerPerRedisServerID[redisServerID] = listener
		s.listenerPerRedisServerIDMutex.Unlock()
	}

	portNum := listener.Addr().(*net.TCPAddr).Port
	response := map[string]int{"port": portNum}
	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)

	if err != nil {
		log.Printf("Error sending response to client: %v", err)
	}

	log.Printf("Responded to /connect request with port %d for redis_server_id %s", portNum, redisServerID)

}

// acceptClients accepts client connections on the given listener for a specific agent
func (s *Server) acceptClients(listener net.Listener, redisServerID string) {
	log.Printf("Starting to accept clients for redisServerID %s on %s", redisServerID, listener.Addr().String())
	for {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Printf("Listener for redisServerID %s closed or error occurred: %v", redisServerID, err)
			return
		}
		log.Printf("Accepted new client connection for redisServerID %s from %s", redisServerID, clientConn.RemoteAddr().String())
		go s.handleClient(clientConn, redisServerID)
	}
}

// handleClient handles TCP connections from clients to the specified agent
func (s *Server) handleClient(clientConn net.Conn, redisServerID string) {

	// Retrieve the agentID associated with the redisServerID
	agentID, err := s.getAgentIDForRedisServerID(redisServerID)

	if err != nil {
		log.Printf("No agent found for redisServerID %s", redisServerID)
		clientConn.Close()
		return
	}

	agent, ok := s.LoadAgent(agentID)
	if !ok {
		log.Printf("Agent %s not found", agentID)
		clientConn.Close()
		return
	}

	clientID := generateUniqueClientID()

	client := &Client{
		ID:             clientID,
		conn:           clientConn,
		send:           make(chan []byte, 256),
		lastActive:     time.Now(),
		RedisServerID:  redisServerID,
		disconnectChan: make(chan struct{}),
	}

	// Add client to agent's client map
	agent.addClient(client)

	// Send __client_connect__ control message to the agent
	controlData := map[string]interface{}{
		"message_type":    "__client_connected__",
		"client_id":       client.ID,
		"redis_server_id": redisServerID,
	}
	// Serialize the control data to JSON
	jsonData, err := json.Marshal(controlData)
	if err != nil {
		log.Printf("Error marshaling connect control message: %v", err)
		clientConn.Close()
		return
	}
	// Create a control message with ClientID == 0
	message := Message{
		ClientID: 0,
		Data:     jsonData,
	}
	agent.send <- message

	// Start the client's write pump
	go client.writePump()

	switch s.clientDisconnectMethod {
	case DISCONNECT_TIMEOUT:
		// Set timeout duration as needed, e.g., 5 minutes
		client.monitorInactivity(5 * time.Minute)
	case DISCONNECT_AUTO:
		s.incrementClientCounter(redisServerID)
	}

	// Read from client and send to agent
	buf := make([]byte, 4096)
	for {
		n, err := client.conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from client: %v", err)
			}
			break // Exit the loop when the client disconnects or an error occurs
		}
		data := make([]byte, n)
		copy(data, buf[:n])

		// Refresh last activity time
		client.lastActive = time.Now()

		// Create a Message with ClientID and Data
		message := Message{
			ClientID: client.ID,
			Data:     data,
		}

		// Send the message to the agent using SendMessage()
		if err := agent.SendMessage(message); err != nil {
			log.Printf("Error sending message to agent %s: %v", agent.agentID, err)
			break // Exit the loop if agent is disconnected or send fails
		}
	}

	// Deferred function will handle client disconnection
	defer func() {
		// Remove client from agent's client map
		agent.removeClient(client)
		client.disconnect()

		log.Printf("Disconnecting client: %s", client.conn.RemoteAddr().String())

		// Decrement client counter if AUTO method is selected
		if s.clientDisconnectMethod == DISCONNECT_AUTO {
			s.decrementClientCounter(redisServerID)
		}

		// Send a __client_disconnect__ control message to the agent
		controlData := map[string]interface{}{
			"message_type": "__client_disconnect__",
			"client_id":    client.ID,
		}
		jsonData, err := json.Marshal(controlData)
		if err != nil {
			log.Printf("Error marshaling disconnect control message: %v", err)
			return
		}
		message := Message{
			ClientID: 0,
			Data:     jsonData,
		}
		agent.send <- message
	}()
}

func (c *Client) monitorInactivity(timeout time.Duration) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if time.Since(c.lastActive) > timeout {
					log.Printf("Client %d inactive for %v, disconnecting", c.ID, timeout)
					c.disconnect()
					return
				}
			case <-c.disconnectChan:
				// Stop monitoring when client is disconnected
				return
			}
		}
	}()
}

// client.writePump sends messages from the agent to the client
func (agent *Agent) writePump() {
	for message := range agent.send {
		writer, err := agent.conn.NextWriter(websocket.BinaryMessage)
		if err != nil {
			// Handle error
			return
		}
		// Write ClientID (8 bytes)
		err = binary.Write(writer, binary.BigEndian, message.ClientID)
		if err != nil {
			writer.Close()
			return
		}
		// Write MessageLength (4 bytes)
		length := uint32(len(message.Data))
		err = binary.Write(writer, binary.BigEndian, length)
		if err != nil {
			writer.Close()
			return
		}
		// Write MessageData
		_, err = writer.Write(message.Data)
		if err != nil {
			writer.Close()
			return
		}
		writer.Close()
	}
}

// handleDisconnectForAgent closes the listener for the given agentID
func (s *Server) handleDisconnectForAgent(agentID string) error {
	log.Printf("Disconnecting agent %s - TODO: nothing done", agentID)
	return nil
}

func (s *Server) StoreAgent(agentID string, agent *Agent) {
	s.agents.Store(agentID, agent)
}

func (s *Server) LoadAgent(agentID string) (*Agent, bool) {
	value, ok := s.agents.Load(agentID)
	if !ok {
		return nil, false
	}
	return value.(*Agent), true
}

func (agent *Agent) setupPingPong() {
	agent.conn.SetReadDeadline(time.Now().Add(agent.pongWait))
	agent.conn.SetPongHandler(func(string) error {
		agent.conn.SetReadDeadline(time.Now().Add(agent.pongWait))
		return nil
	})
}

func (agent *Agent) startPing() {
	ticker := time.NewTicker(agent.pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-agent.done:
			return
		case <-ticker.C:
			if err := agent.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				// Handle disconnection
				log.Printf("Agent %s disconnecting due to ping timeout", agent.agentID)
				agent.disconnect()
				return
			}
		}
	}
}

func (agent *Agent) disconnect() {
	agent.mu.Lock()
	if !agent.connected {
		agent.mu.Unlock()
		return
	}
	agent.connected = false
	agent.mu.Unlock()

	close(agent.done)
	agent.conn.Close()

	// Close agent.send to signal writePump to exit
	close(agent.send)

	// Disconnect all clients
	agent.clientsMux.Lock()
	for _, client := range agent.clients {
		client.conn.Close()
	}
	agent.clientsMux.Unlock()

	// Handle agent disconnect
	agent.server.handleDisconnectForAgent(agent.agentID)
}

func readMessage(reader io.Reader) (Message, error) {
	var msg Message
	// Read ClientID (8 bytes)
	err := binary.Read(reader, binary.BigEndian, &msg.ClientID)
	if err != nil {
		return msg, err
	}
	// Read MessageLength (4 bytes)
	var length uint32
	err = binary.Read(reader, binary.BigEndian, &length)
	if err != nil {
		return msg, err
	}
	// Read MessageData
	msg.Data = make([]byte, length)
	_, err = io.ReadFull(reader, msg.Data)
	if err != nil {
		return msg, err
	}
	return msg, nil
}

// handleDisconnectFromRedisServer handles the API request to disconnect from an agent
func (s *Server) handleDisconnectFromRedisServer(w http.ResponseWriter, r *http.Request) {
	redisServerID := r.URL.Query().Get("redis_server_id")
	log.Printf("Received /disconnect request for redis_server_id: %s", redisServerID)
	if redisServerID == "" {
		http.Error(w, "redis_server_id is required", http.StatusBadRequest)
		return
	}

	// Proceed to disconnect the agent
	err := s.handleDisconnectForRedisServerID(redisServerID)
	if err != nil {
		log.Printf("Error disconnecting redis_server_id %s: %v", redisServerID, err)
		http.Error(w, "Failed to disconnect", http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully disconnected redis_server_id %s", redisServerID)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Disconnected successfully"))
}

func (s *Server) createListenerForRedisServerID(redisServerID string) net.Listener {
	// Find an available port
	var listener net.Listener
	var err error
	for port := s.basePort; port < 65535; port++ {
		address := "127.0.0.1:" + strconv.Itoa(port)
		listener, err = net.Listen("tcp", address)
		if err == nil {
			log.Printf("Allocated port %d for redisServerID %s", port, redisServerID)
			break
		}
	}
	if listener == nil {
		log.Printf("No available ports to allocate for redisServerID %s", redisServerID)
		return nil
	}

	log.Printf("Listener started on %s for redisServerID %s", listener.Addr().String(), redisServerID)

	// Start accepting client connections on the new port
	go s.acceptClients(listener, redisServerID)
	return listener
}

// Add a client to the agent
func (agent *Agent) addClient(client *Client) {
	agent.clientsMux.Lock()
	defer agent.clientsMux.Unlock()
	agent.clients[client.ID] = client
}

// Remove a client from the agent
func (agent *Agent) removeClient(client *Client) {
	agent.clientsMux.Lock()
	defer agent.clientsMux.Unlock()
	delete(agent.clients, client.ID)
	client.disconnectOnce.Do(func() {
		close(client.send)
	})
}

func (client *Client) writePump() {
	for data := range client.send {
		_, err := client.conn.Write(data)
		if err != nil {
			// Handle error
			break
		}
	}
}

func (s *Server) handleDisconnectForRedisServerID(redisServerID string) error {
	// Close the listener for redisServerID
	s.listenerPerRedisServerIDMutex.Lock()
	listener, exists := s.listenerPerRedisServerID[redisServerID]
	if exists {
		log.Printf("Closing listener for redisServerID %s", redisServerID)
		err := listener.Close()
		if err != nil {
			log.Printf("Error closing listener for redisServerID %s: %v", redisServerID, err)
			s.listenerPerRedisServerIDMutex.Unlock()
			return err
		}
		delete(s.listenerPerRedisServerID, redisServerID)
		log.Printf("Listener for redisServerID %s has been closed and removed from the map", redisServerID)
	} else {
		log.Printf("Listener for redisServerID %s not found", redisServerID)
	}
	s.listenerPerRedisServerIDMutex.Unlock()

	// Now disconnect clients associated with the redisServerID
	agentID, err := s.getAgentIDForRedisServerID(redisServerID)

	if err != nil {
		log.Printf("No agent found for redisServerID %s", redisServerID)
		return nil // Or return an error if necessary
	}

	agent, ok := s.LoadAgent(agentID)
	if !ok {
		log.Printf("Agent %s not found", agentID)
		return nil // Or return an error if necessary
	}

	// Instruct the agent to disconnect clients associated with redisServerID
	agent.disconnectClients(redisServerID)

	return nil
}

// In Agent struct, add the disconnectClients method
func (agent *Agent) disconnectClients(redisServerID string) {
	// Collect client IDs to disconnect
	agent.clientsMux.Lock()
	clientsToDisconnect := make([]uint64, 0)
	for clientID, client := range agent.clients {
		if client.RedisServerID == redisServerID {
			clientsToDisconnect = append(clientsToDisconnect, clientID)
		}
	}
	agent.clientsMux.Unlock()

	// Now disconnect the clients
	for _, clientID := range clientsToDisconnect {
		agent.clientsMux.RLock()
		client, ok := agent.clients[clientID]
		agent.clientsMux.RUnlock()
		if ok {
			client.disconnect()
			agent.removeClient(client)
		}
	}
}

// In Client struct, implement a disconnect method
func (client *Client) disconnect() {
	client.disconnectOnce.Do(func() {
		close(client.disconnectChan)
		client.conn.Close()
		close(client.send)
	})
}

func (agent *Agent) SendMessage(message Message) error {
	agent.mu.Lock()
	connected := agent.connected
	agent.mu.Unlock()

	if !connected {
		return fmt.Errorf("agent is disconnected")
	}

	select {
	case agent.send <- message:
		return nil
	default:
		return fmt.Errorf("agent send channel is full or closed")
	}
}

// enableCors is a middleware that sets the CORS headers
func enableCors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Adjust the allowed origin as needed. "*" allows all origins.
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) incrementClientCounter(redisServerID string) {
	if s.clientDisconnectMethod == DISCONNECT_AUTO {
		s.activeClientsMutex.Lock()
		s.activeClients[redisServerID]++
		s.activeClientsMutex.Unlock()
	}
}

func (s *Server) decrementClientCounter(redisServerID string) {
	if s.clientDisconnectMethod == DISCONNECT_AUTO {
		s.activeClientsMutex.Lock()
		s.activeClients[redisServerID]--
		count := s.activeClients[redisServerID]
		s.activeClientsMutex.Unlock()

		// Check if count is zero and close the listener if needed
		if count == 0 {
			s.closeListenerIfNoClients(redisServerID)
		}
	}
}

// closeListenerIfNoClients checks if there are no active clients for the given redisServerID
// and closes the listener if that's the case.
func (s *Server) closeListenerIfNoClients(redisServerID string) {
	s.activeClientsMutex.Lock()
	count := s.activeClients[redisServerID]
	s.activeClientsMutex.Unlock()

	if count > 0 {
		// There are still active clients; no action needed.
		return
	}

	// Close the listener for redisServerID
	s.listenerPerRedisServerIDMutex.Lock()
	listener, exists := s.listenerPerRedisServerID[redisServerID]
	if exists {
		log.Printf("Closing listener for redisServerID %s as there are no active clients", redisServerID)
		err := listener.Close()
		if err != nil {
			log.Printf("Error closing listener for redisServerID %s: %v", redisServerID, err)
		}
		delete(s.listenerPerRedisServerID, redisServerID)
	}
	s.listenerPerRedisServerIDMutex.Unlock()
}
