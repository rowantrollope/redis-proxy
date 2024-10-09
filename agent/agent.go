// agent.go

package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SERVER_URL = "ws://127.0.0.1:8080/agent"
)

// Message represents a message with ClientID and Data
type Message struct {
	ClientID uint64
	Data     []byte
}

type RedisClient struct {
	ClientID      uint64
	RedisConn     net.Conn
	Reader        *bufio.Reader
	Writer        *bufio.Writer
	cleanupOnce   sync.Once
	clientManager *ClientManager // Add this field
}

type RequestManager struct {
	pendingRequests map[string]chan []byte
	mutex           sync.Mutex
}

type ClientManager struct {
	clients                  map[uint64]*RedisClient
	clientsMutex             sync.Mutex
	redisServerDetailsMap    map[string]RedisServerDetails
	redisServerDetailsMutex  sync.Mutex
	clientRedisServerIDMap   map[uint64]string
	clientRedisServerIDMutex sync.Mutex
}

// wsConn is the WebSocket connection to the server
type Agent struct {
	wsConn             *websocket.Conn
	writeMutex         sync.Mutex
	heartbeatStopChans map[string]chan struct{}
	heartbeatMutex     sync.Mutex
}

// RedisServerDetails holds connection details for a Redis server
type RedisServerDetails struct {
	Host     string
	Port     string
	Username string
	Password string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Generate a UUID for the agent
	agentID, err := getOrCreateAgentID()
	if err != nil {
		log.Fatalf("Failed to get or create agent ID: %v", err)
	}

	// Connect to the server via WebSocket
	wsConn, err := connectToServer(agentID)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}

	// Create an Agent instance
	agent := &Agent{
		wsConn:             wsConn,
		writeMutex:         sync.Mutex{},
		heartbeatStopChans: make(map[string]chan struct{}),
		heartbeatMutex:     sync.Mutex{},
	}

	// Initialize RequestManager
	requestManager := &RequestManager{
		pendingRequests: make(map[string]chan []byte),
	}
	// Load Redis server details
	redisDetailsMap, err := loadRedisServerDetails()
	if err != nil {
		log.Fatalf("Failed to load Redis server details: %v", err)
	}

	log.Printf("Redis server details: %v", redisDetailsMap)
	clientManager := &ClientManager{
		clients:                make(map[uint64]*RedisClient),
		redisServerDetailsMap:  redisDetailsMap,
		clientRedisServerIDMap: make(map[uint64]string),
	}

	// Start the HTTP server for activation requests
	go startHTTPServer(requestManager, clientManager, agent)

	// Start handling messages from the server
	handleServerMessages(requestManager, clientManager, agent)

	// Start the heartbeat for existing Redis servers
	agent.startRedisServerHeartbeats(clientManager)

	// inform the server about the Redis server IDs that this agent can manage
	for redisServerID := range clientManager.redisServerDetailsMap {
		err := agent.sendRedisServerID(redisServerID)
		if err != nil {
			log.Printf("Failed to inform server about redisServerID %s: %v", redisServerID, err)
		}
	}

	// Block forever
	select {}
}

// connectToServer establishes a WebSocket connection to the server
func connectToServer(agentID string) (*websocket.Conn, error) {
	u, err := url.Parse(SERVER_URL)
	if err != nil {
		return nil, err
	}

	log.Printf("Connecting to server at %s", u.String())

	wsConnTemp, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}

	// Send the agent ID to the server
	err = wsConnTemp.WriteMessage(websocket.TextMessage, []byte(agentID))
	if err != nil {
		wsConnTemp.Close()
		return nil, err
	}

	log.Printf("Connected to server with Agent ID: %s", agentID)

	return wsConnTemp, nil
}

func startHTTPServer(requestManager *RequestManager, clientManager *ClientManager, agent *Agent) {
	http.HandleFunc("/activate", func(w http.ResponseWriter, r *http.Request) {
		activateHandler(w, r, requestManager, clientManager, agent)
	})

	log.Println("Starting HTTP server on :8081")
	err := http.ListenAndServe(":8081", nil)
	if err != nil {
		log.Fatal("HTTP server error:", err)
	}
}

// activateHandler handles the /activate HTTP endpoint
func activateHandler(w http.ResponseWriter, r *http.Request, requestManager *RequestManager, clientManager *ClientManager, agent *Agent) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		UUID     string `json:"uuid"`
		Host     string `json:"host"`
		Port     string `json:"port"`
		Username string `json:"username"`
		Password string `json:"password"`
	}

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Println("Error decoding activation request:", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.UUID == "" {
		http.Error(w, "UUID is required", http.StatusBadRequest)
		return
	}

	// Provide default values if not specified
	if req.Host == "" || req.Host == "localhost" {
		req.Host = "127.0.0.1"
	}
	if req.Port == "" {
		req.Port = "6379"
	}

	// Log the activation request
	log.Println("Received activation request with uuid:", req.UUID, "host:", req.Host, "port:", req.Port)

	// **Add the Redis server details to the clientManager**
	redisDetails := RedisServerDetails{
		Host:     req.Host,
		Port:     req.Port,
		Username: req.Username,
		Password: req.Password,
	}

	clientManager.redisServerDetailsMutex.Lock()
	clientManager.redisServerDetailsMap[req.UUID] = redisDetails
	clientManager.redisServerDetailsMutex.Unlock()

	// Save the updated Redis server details to disk
	err = saveRedisServerDetails(clientManager.redisServerDetailsMap)
	if err != nil {
		log.Println("Error saving Redis server details:", err)
		// Handle error appropriately
	}

	// Generate a unique request ID
	requestID := uuid.New().String()
	redisServerID := req.UUID 

	// Create a response channel and store it in the pending requests map
	respChan := make(chan []byte)
	requestManager.mutex.Lock()
	requestManager.pendingRequests[requestID] = respChan
	requestManager.mutex.Unlock()

	// Create the activation control message
	controlMsg := map[string]interface{}{
		"message_type":    "__activation_request__",
		"redis_server_id": redisServerID,
		"request_id":      requestID,
	}
	controlMsgBytes, err := json.Marshal(controlMsg)
	if err != nil {
		log.Println("Error marshaling activation control message:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Send the control message to the server
	err = agent.sendControlMessage(controlMsgBytes)
	if err != nil {
		log.Println("Error sending activation control message to server:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Start a heartbeat for the new Redis server
	agent.startHeartbeat(redisServerID, clientManager)

	// Wait for the server's response or timeout
	select {
	case respData := <-respChan:
		// Send the server's response back to the client
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(respData)
	case <-time.After(10 * time.Second):
		log.Println("Timeout waiting for server response")
		// Clean up the pending request
		requestManager.mutex.Lock()
		delete(requestManager.pendingRequests, requestID)
		requestManager.mutex.Unlock()
		http.Error(w, "Timeout waiting for server response", http.StatusGatewayTimeout)
		return
	}
}

// agent.go
func getOrCreateAgentID() (string, error) {
	const agentIDFile = "agent_id.txt"
	if _, err := os.Stat(agentIDFile); err == nil {
		// Agent ID file exists
		data, err := os.ReadFile(agentIDFile)
		if err != nil {
			return "", err
		}
		agentID := strings.TrimSpace(string(data))
		log.Printf("Using existing agent ID: %s", agentID)
		return agentID, nil
	}

	// Create a new agent ID and save it
	agentID := uuid.New().String()
	err := os.WriteFile(agentIDFile, []byte(agentID), 0600)
	if err != nil {
		return "", err
	}
	log.Printf("Generated new agent ID: %s", agentID)
	return agentID, nil
}

// sendControlMessage sends a control message to the server
func (agent *Agent) sendControlMessage(data []byte) error {
	agent.writeMutex.Lock()
	defer agent.writeMutex.Unlock()

	if agent.wsConn == nil {
		return fmt.Errorf("WebSocket connection is not established")
	}

	msg := Message{
		ClientID: 0,
		Data:     data,
	}

	// Write the message to the server with headers
	return writeMessage(agent.wsConn, msg)
}

// handleServerMessages reads messages from the server
func handleServerMessages(requestManager *RequestManager, clientManager *ClientManager, agent *Agent) {
	go func() {
		for {
			wsConn := agent.wsConn
			if wsConn == nil {
				time.Sleep(1 * time.Second)
				continue
			}

			// Read message from server with headers
			messageType, reader, err := wsConn.NextReader()
			if err != nil {
				log.Printf("Error reading from server: %v", err)
				agent.reconnectToServer()
				continue
			}

			if messageType != websocket.BinaryMessage {
				log.Printf("Expected binary message, got type %d", messageType)
				continue
			}

			message, err := readMessage(reader)
			if err != nil {
				log.Printf("Error reading message: %v", err)
				continue
			}

			clientID := message.ClientID
			data := message.Data

			if clientID == 0 {
				// This is a control message
				err := handleControlMessage(data, requestManager, clientManager, agent)
				if err != nil {
					log.Printf("Error handling control message: %v", err)
				}
			} else {
				// Handle the message for the client
				handleClientMessage(clientID, data, clientManager)
			}
		}
	}()
}

// handleControlMessage handles control messages received from the server
func handleControlMessage(data []byte, requestManager *RequestManager, clientManager *ClientManager, agent *Agent) error {
	var controlMsg map[string]interface{}
	err := json.Unmarshal(data, &controlMsg)
	if err != nil {
		return fmt.Errorf("error parsing control message JSON: %v", err)
	}

	messageType, ok := controlMsg["message_type"].(string)
	if !ok {
		return fmt.Errorf("control message missing 'message_type' field or it is not a string")
	}

	switch messageType {
	case "__activation_response__":
		// Handle the activation response
		return handleActivationResponse(controlMsg, requestManager)
	case "__activation_claimed__":
		return handleActivationComplete(controlMsg, clientManager, agent)
	case "__deactivate_redis_server__":
		return handleDeactivateRedisServer(controlMsg, clientManager, agent)
	case "__client_connected__":
		return handleClientConnected(controlMsg, clientManager, agent)
	case "__client_disconnect__":
		// Handle the disconnect message
		return handleClientDisconnected(controlMsg, clientManager)
	default:
		return fmt.Errorf("unknown control message_type: %s", messageType)
	}
}

func handleDeactivateRedisServer(controlMsg map[string]interface{}, clientManager *ClientManager, agent *Agent) error {
	// Extract redis_server_id from control message
	redisServerID, ok := controlMsg["redis_server_id"].(string)
	if !ok {
		return fmt.Errorf("deactivate redis server message missing 'redis_server_id' field or it is not a string")
	}

	log.Printf("Received request to deactivate Redis server ID: %s", redisServerID)

	// Stop the heartbeat for the redisServerID
	agent.stopHeartbeat(redisServerID)

	// Remove the Redis server details from the clientManager
	clientManager.redisServerDetailsMutex.Lock()
	delete(clientManager.redisServerDetailsMap, redisServerID)
	clientManager.redisServerDetailsMutex.Unlock()

	// Save the updated Redis server details to disk
	err := saveRedisServerDetails(clientManager.redisServerDetailsMap)
	if err != nil {
		log.Printf("Error saving Redis server details after deactivation: %v", err)
	}

	// Disconnect any clients associated with this redisServerID
	clientManager.disconnectClientsForRedisServerID(redisServerID)

	return nil
}

func (cm *ClientManager) disconnectClientsForRedisServerID(redisServerID string) {
	cm.clientsMutex.Lock()
	defer cm.clientsMutex.Unlock()
	for clientID, client := range cm.clients {
		cm.clientRedisServerIDMutex.Lock()
		clientRedisServerID, exists := cm.clientRedisServerIDMap[clientID]
		cm.clientRedisServerIDMutex.Unlock()

		if exists && clientRedisServerID == redisServerID {
			client.cleanup()
		}
	}
}
func handleActivationComplete(controlMsg map[string]interface{}, clientManager *ClientManager, agent *Agent) error {
	// Extract redis_server_id from control message
	_, ok := controlMsg["redis_server_id"].(string)
	if !ok {
		return fmt.Errorf("activation complete message missing 'redis_server_id' field or it is not a string")
	}
	
	log.Printf("Activation complete for Redis server ID: %s", controlMsg["redis_server_id"])
	
	return nil
}

// handleActivationResponse handles the activation response from the server
func handleActivationResponse(controlMsg map[string]interface{}, requestManager *RequestManager) error {
	// Extract request_id (to send response back to HTTP client)
	requestID, ok := controlMsg["request_id"].(string)
	if !ok {
		return fmt.Errorf("activation response missing 'request_id' field or it is not a string")
	}

	// Send the data back to the HTTP client
	data, ok := controlMsg["data"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("activation response missing 'data' field or it is not a map")
	}

	responseData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshaling activation response: %v", err)
	}
	log.Printf("Received activation response: %s", string(responseData))

	requestManager.mutex.Lock()
	respChan, exists := requestManager.pendingRequests[requestID]
	if exists {
		respChan <- responseData
		close(respChan)
		delete(requestManager.pendingRequests, requestID)
	} else {
		log.Printf("No pending request found for request_id: %s", requestID)
	}
	requestManager.mutex.Unlock()

	return nil
}

func handleClientDisconnected(controlMsg map[string]interface{}, clientManager *ClientManager) error {
	// Extract client_id from control message
	clientIDFloat, ok := controlMsg["client_id"].(float64)
	if !ok {
		return fmt.Errorf("disconnect message missing 'client_id' field or it is not a number")
	}
	clientID := uint64(clientIDFloat)

	// Safely clean up the client's resources
	clientManager.clientsMutex.Lock()
	client, exists := clientManager.clients[clientID]
	clientManager.clientsMutex.Unlock()

	if exists {
		client.cleanup()
		log.Printf("Client %d disconnected, cleaned up Redis connection", clientID)
	} else {
		log.Printf("Client %d not found during disconnect", clientID)
	}

	return nil
}

// handleClientMessage handles data messages for the clients
func handleClientMessage(clientID uint64, data []byte, clientManager *ClientManager) {
	clientManager.clientsMutex.Lock()
	client, exists := clientManager.clients[clientID]
	clientManager.clientsMutex.Unlock()
	if !exists {
		log.Printf("Received message for unknown ClientID %d", clientID)
		return
	}

	// Write the data to the client's Redis connection
	_, err := client.Writer.Write(data)
	if err != nil {
		log.Printf("Error writing to Redis for ClientID %d: %v", clientID, err)
		return
	}
	err = client.Writer.Flush()
	if err != nil {
		log.Printf("Error flushing Redis writer for ClientID %d: %v", clientID, err)
		return
	}
}

// readFromRedis reads responses from Redis and sends them back to the server
func readFromRedis(client *RedisClient, agent *Agent) {
	defer func() {
		client.cleanup()
	}()

	for {
		// Read Redis response
		response, err := readRedisResponse(client.Reader)
		if err != nil {
			if err == io.EOF {
				log.Printf("Redis connection closed for ClientID %d", client.ClientID)
			} else {
				log.Printf("Error reading from Redis for ClientID %d: %v", client.ClientID, err)
			}
			return
		}

		// Create a message to send back to the server
		message := Message{
			ClientID: client.ClientID,
			Data:     response,
		}

		// Write the message to the server with headers using agent
		err = agent.sendMessage(message)
		if err != nil {
			log.Printf("Error writing to server for ClientID %d: %v", client.ClientID, err)
			return
		}
	}
}

func (agent *Agent) sendMessage(msg Message) error {
	agent.writeMutex.Lock()
	defer agent.writeMutex.Unlock()

	if agent.wsConn == nil {
		return fmt.Errorf("WebSocket connection is not established")
	}

	// Write the message to the server with headers
	return writeMessage(agent.wsConn, msg)
}

// readMessage reads a Message from an io.Reader
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
	//    log.Printf("Received message for ClientID: %d, length: %d", msg.ClientID, length)

	// Read MessageData
	msg.Data = make([]byte, length)
	_, err = io.ReadFull(reader, msg.Data)
	if err != nil {
		return msg, err
	}

	return msg, nil
}

// writeMessage writes a Message to the WebSocket connection with headers
func writeMessage(wsConn *websocket.Conn, msg Message) error {
	writer, err := wsConn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}
	defer writer.Close()

	// Write ClientID (8 bytes)
	err = binary.Write(writer, binary.BigEndian, msg.ClientID)
	if err != nil {
		return err
	}

	// Write MessageLength (4 bytes)
	length := uint32(len(msg.Data))
	err = binary.Write(writer, binary.BigEndian, length)
	if err != nil {
		return err
	}

	// Write MessageData
	_, err = writer.Write(msg.Data)
	if err != nil {
		return err
	}

	return nil
}

// readRedisResponse reads a complete response from Redis
func readRedisResponse(reader *bufio.Reader) ([]byte, error) {
	var response []byte
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		response = append(response, line...)
		// Check if the response is complete based on Redis protocol
		if isCompleteRedisResponse(response) {
			break
		}
	}
	return response, nil
}

// isCompleteRedisResponse determines if response is complete
func isCompleteRedisResponse(response []byte) bool {
	// This function should parse the response and determine if it's complete
	// Implement proper RESP protocol parsing

	// For simplicity, assume every line is a complete response
	return true
}

// reconnectToServer attempts to reconnect to the server
func (agent *Agent) reconnectToServer() {
	if agent.wsConn != nil {
		agent.wsConn.Close()
	}
	agent.wsConn = nil

	// Attempt to reconnect every 5 seconds
	for {
		time.Sleep(5 * time.Second)
		log.Println("Attempting to reconnect to server...")
		wsConn, err := connectToServer(uuid.New().String())
		if err != nil { 
			log.Printf("Reconnect failed: %v", err)
			continue
		}
		agent.wsConn = wsConn
		log.Println("Reconnected to server")
		break
	}
}

// handleClientConnected handles the client connected message from the server
func handleClientConnected(controlMsg map[string]interface{}, clientManager *ClientManager, agent *Agent) error {
	// Extract client_id from control message
	clientIDFloat, ok := controlMsg["client_id"].(float64)
	if !ok {
		return fmt.Errorf("client connected message missing 'client_id' field or it is not a number")
	}
	clientID := uint64(clientIDFloat)

	// Extract redis_server_id from control message
	redisServerID, ok := controlMsg["redis_server_id"].(string)
	if !ok {
		return fmt.Errorf("client connected message missing 'redis_server_id' field or it is not a string")
	}

	// Map clientID to redis-server-id
	clientManager.clientRedisServerIDMutex.Lock()
	clientManager.clientRedisServerIDMap[clientID] = redisServerID
	clientManager.clientRedisServerIDMutex.Unlock()

	// Retrieve the Redis server details
	clientManager.redisServerDetailsMutex.Lock()
	redisDetails, ok := clientManager.redisServerDetailsMap[redisServerID]
	clientManager.redisServerDetailsMutex.Unlock()
	if !ok {
		log.Printf("No Redis server details found for redis-server-id %s", redisServerID)
		return fmt.Errorf("No Redis server details found for redis-server-id %s", redisServerID)
	}

	// Build the address string
	address := net.JoinHostPort(redisDetails.Host, redisDetails.Port)

	// Create a new Redis connection for this client
	redisConn, err := net.Dial("tcp", address)
	if err != nil {
		log.Printf("Error connecting to Redis for ClientID %d at %s: %v", clientID, address, err)
		return err
	}

	client := &RedisClient{
		ClientID:      clientID,
		RedisConn:     redisConn,
		Reader:        bufio.NewReader(redisConn),
		Writer:        bufio.NewWriter(redisConn),
		clientManager: clientManager,
	}

	// Store the client in clients map
	clientManager.clientsMutex.Lock()
	clientManager.clients[clientID] = client
	clientManager.clientsMutex.Unlock()

	// Start a goroutine to read responses from Redis for this client
	go readFromRedis(client, agent)

	log.Printf("Client %d connected and Redis connection established", clientID)

	return nil
}

func (client *RedisClient) cleanup() {
	client.cleanupOnce.Do(func() {
		// Close the Redis connection
		client.RedisConn.Close()

		// Remove the client from clients map
		client.clientManager.clientsMutex.Lock()
		delete(client.clientManager.clients, client.ClientID)
		client.clientManager.clientsMutex.Unlock()

		// Remove the clientID mapping from clientRedisServerIDMap
		client.clientManager.clientRedisServerIDMutex.Lock()
		delete(client.clientManager.clientRedisServerIDMap, client.ClientID)
		client.clientManager.clientRedisServerIDMutex.Unlock()

		log.Printf("Cleaned up Redis client for ClientID %d", client.ClientID)
	})
}

// Load existing Redis server details on startup
func loadRedisServerDetails() (map[string]RedisServerDetails, error) {
	const fileName = "redis_servers.json"
	data, err := os.ReadFile(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, return empty map
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

// Save Redis server details to file
func saveRedisServerDetails(detailsMap map[string]RedisServerDetails) error {
	const fileName = "redis_servers.json"
	data, err := json.MarshalIndent(detailsMap, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(fileName, data, 0600)
}

func (agent *Agent) sendRedisServerID(redisServerID string) error {
	controlMsg := map[string]interface{}{
		"message_type":    "__agent_managed_redis_server__",
		"redis_server_id": redisServerID,
	}
	controlMsgBytes, err := json.Marshal(controlMsg)
	if err != nil {
		return err
	}
	return agent.sendControlMessage(controlMsgBytes)
}

// startRedisServerHeartbeats starts heartbeats for all Redis servers
func (agent *Agent) startRedisServerHeartbeats(clientManager *ClientManager) {
	clientManager.redisServerDetailsMutex.Lock()
	defer clientManager.redisServerDetailsMutex.Unlock()

	for redisServerID := range clientManager.redisServerDetailsMap {
		agent.startHeartbeat(redisServerID, clientManager)
	}
}

// startHeartbeat starts a heartbeat goroutine for a specific Redis server
func (agent *Agent) startHeartbeat(redisServerID string, clientManager *ClientManager) {
	agent.heartbeatMutex.Lock()
	// Check if a heartbeat is already running for this Redis server
	if _, exists := agent.heartbeatStopChans[redisServerID]; exists {
		agent.heartbeatMutex.Unlock()
		return
	}
	// Create a stop channel for the heartbeat
	stopChan := make(chan struct{})
	agent.heartbeatStopChans[redisServerID] = stopChan
	agent.heartbeatMutex.Unlock()

	go func() {
		ticker := time.NewTicker(10 * time.Second) // Adjust the interval as needed
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				agent.performHeartbeat(redisServerID, clientManager)
			case <-stopChan:
				// Heartbeat stopped
				return
			}
		}
	}()
}

// performHeartbeat performs the heartbeat logic for a Redis server
func (agent *Agent) performHeartbeat(redisServerID string, clientManager *ClientManager) {
	// Retrieve the Redis server details
	clientManager.redisServerDetailsMutex.Lock()
	redisDetails, exists := clientManager.redisServerDetailsMap[redisServerID]
	clientManager.redisServerDetailsMutex.Unlock()

	if !exists {
		// Redis server no longer exists; stop the heartbeat
		agent.stopHeartbeat(redisServerID)
		return
	}

	// Collect system stats
    systemStats := make(map[string]interface{})

    // Get hostname
    hostname, err := os.Hostname()
    if err != nil {
        hostname = "unknown"
    }
    systemStats["hostname"] = hostname

    // Get IP addresses
    ips, err := getLocalIPAddresses()
    if err != nil {
        ips = []string{}
    }
    systemStats["ip_addresses"] = ips


	// Try to connect to the Redis server
	address := net.JoinHostPort(redisDetails.Host, redisDetails.Port)
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	status := "OFFLINE"
	stats := "{}"

	timestamp := time.Now().Unix()
	if err == nil {
		defer conn.Close()
		// Set read deadline
		conn.SetReadDeadline(time.Now().Add(5 * time.Second)) // Adjusted timeout

		// Create a buffered reader for the connection
		reader := bufio.NewReader(conn)

		// Send AUTH command to Redis IF we have a password
		if redisDetails.Password != "" {
			authCmd := fmt.Sprintf("AUTH %s %s\r\n", redisDetails.Username, redisDetails.Password)
			_, err = conn.Write([]byte(authCmd))
			if err != nil {
				log.Printf("Error sending AUTH command to Redis: %v", err)
				return
			}

			// Read response from AUTH command
			authResponse, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("Error reading AUTH response from Redis: %v", err)
				return
			}

			if strings.HasPrefix(authResponse, "-ERR") {
				log.Printf("AUTH failed: %s", authResponse)
				return
			}
		}

		// Send INFO command to Redis
		_, err = conn.Write([]byte("INFO\r\n"))
		if err != nil {
			log.Printf("Error sending INFO command to Redis: %v", err)
			return
		}

		// Read the INFO response as a bulk string
		infoResponse, err := readBulkString(reader)
		if err != nil {
			log.Printf("Error reading INFO response from Redis: %v", err)
			return
		}

		// Parse the INFO response into a JSON object
		infoMap, err := parseInfoResponse(infoResponse)
		if err != nil {
			log.Printf("Error parsing INFO response: %v", err)
			return
		}

		jsonData, err := json.Marshal(infoMap)
		if err != nil {
			log.Printf("Error marshaling INFO response to JSON: %v", err)
			return
		}

		status = "RUNNING"
		stats = string(jsonData)
	}

	// Prepare the control message
	controlMsg := map[string]interface{}{
		"message_type":    "__redis_server_heartbeat__",
		"redis_server_id": redisServerID,
		"status":          status,
		"timestamp":       timestamp,
		"redis_stats":     stats,
		"system_stats":    systemStats,
	}
	controlMsgBytes, err := json.Marshal(controlMsg)
	if err != nil {
		log.Printf("Error marshaling heartbeat control message for Redis server %s: %v", redisServerID, err)
		return
	}

	// Send the control message to the server
	err = agent.sendControlMessage(controlMsgBytes)
	if err != nil {
		log.Printf("Error sending heartbeat control message for Redis server %s: %v", redisServerID, err)
	}
}

// stopHeartbeat stops the heartbeat for a specific Redis server
func (agent *Agent) stopHeartbeat(redisServerID string) {
	agent.heartbeatMutex.Lock()
	defer agent.heartbeatMutex.Unlock()
	if stopChan, exists := agent.heartbeatStopChans[redisServerID]; exists {
		close(stopChan)
		delete(agent.heartbeatStopChans, redisServerID)
	}
}

// getLocalIPAddresses retrieves the non-loopback IP addresses of the host
func getLocalIPAddresses() ([]string, error) {
    var ips []string
    interfaces, err := net.Interfaces()
    if err != nil {
        return nil, err
    }
    for _, i := range interfaces {
        addrs, err := i.Addrs()
        if err != nil {
            continue
        }
        for _, addr := range addrs {
            var ip net.IP
            switch v := addr.(type) {
            case *net.IPNet:
                ip = v.IP
            case *net.IPAddr:
                ip = v.IP
            }
            if ip == nil || ip.IsLoopback() {
                continue
            }
            ips = append(ips, ip.String())
        }
    }
    return ips, nil
}
// readBulkString reads a bulk string from the Redis server
func readBulkString(reader *bufio.Reader) (string, error) {
	// Read the first line, which contains the '$' and length
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	if len(line) == 0 {
		return "", fmt.Errorf("empty response")
	}

	if line[0] != '$' {
		return "", fmt.Errorf("expected bulk string ('$'), got %q", line[0])
	}

	// Get the length
	lengthStr := strings.TrimSpace(line[1:])
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %v", err)
	}

	if length == -1 {
		// Null bulk string
		return "", nil
	}

	// Read the data
	buf := make([]byte, length+2) // +2 for \r\n
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return "", err
	}

	// Remove the trailing \r\n
	return string(buf[:length]), nil
}

// parseInfoResponse parses the INFO response into a flat map without section headings
func parseInfoResponse(info string) (map[string]interface{}, error) {
	lines := strings.Split(info, "\n")
	result := make(map[string]interface{})

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue // Skip empty lines
		}
		if strings.HasPrefix(line, "#") {
			// Ignore section header
			continue
		}
		// Parse key-value pairs
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue // Skip invalid lines
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		result[key] = value
	}

	return result, nil
}