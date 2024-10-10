package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
	"io"
	"net"
	"os"
	"strings"
	"strconv"
	"bufio"
	"net/url"
	"net/http"

	"github.com/rowantrollope/redis-proxy/agent/redisclient"
	"github.com/rowantrollope/redis-proxy/agent/common"
	
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
    SERVER_URL = "ws://127.0.0.1:8080/agent"
)

type Agent struct {
	wsConn             *websocket.Conn
	writeMutex         sync.Mutex
	heartbeatStopChans map[string]chan struct{}
	heartbeatMutex     sync.Mutex
}

func NewAgent() *Agent {
	// Generate or retrieve the agent ID
    agentID, err := getOrCreateAgentID()
    if err != nil {
        log.Fatalf("Failed to get or create agent ID: %v", err)
    }

    // Connect to the server via WebSocket
    wsConn, err := ConnectToServer(agentID, SERVER_URL)
    if err != nil {
        log.Fatalf("Failed to connect to server: %v", err)
    }

	return &Agent{
		wsConn:             wsConn,
		heartbeatStopChans: make(map[string]chan struct{}),
	}
}

func ConnectToServer(agentID string, serverURL string) (*websocket.Conn, error) {
    u, err := url.Parse(serverURL)
    if err != nil {
        return nil, err
    }

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

    return wsConnTemp, nil
}

func getOrCreateAgentID() (string, error) {
    const agentIDFile = "agent_id.txt"
    if _, err := os.Stat(agentIDFile); err == nil {
        data, err := os.ReadFile(agentIDFile)
        if err != nil {
            return "", err
        }
        agentID := strings.TrimSpace(string(data))
        return agentID, nil
    }

    agentID := uuid.New().String()
    err := os.WriteFile(agentIDFile, []byte(agentID), 0600)
    if err != nil {
        return "", err
    }
    return agentID, nil
}

// sendControlMessage sends a control message to the server
func (agent *Agent) sendControlMessage(data []byte) error {
	agent.writeMutex.Lock()
	defer agent.writeMutex.Unlock()

	if agent.wsConn == nil {
		return fmt.Errorf("WebSocket connection is not established")
	}

	msg := common.Message{
		ClientID: 0,
		Data:     data,
	}

	// Write the message to the server with headers
	return common.WriteBinaryMessage(agent.wsConn, msg)
}

func (agent *Agent) sendMessage(msg common.Message) error {
	agent.writeMutex.Lock()
	defer agent.writeMutex.Unlock()

	if agent.wsConn == nil {
		return fmt.Errorf("WebSocket connection is not established")
	}

	// Write the message to the server with headers
	return common.WriteBinaryMessage(agent.wsConn, msg)
}

func (agent *Agent) HandleServerMessages(requestManager *RequestManager, clientManager *redisclient.ClientManager) {
	go func() {
		for {
			if agent.wsConn == nil {
				time.Sleep(1 * time.Second)
				continue
			}

			messageType, reader, err := agent.wsConn.NextReader()
			if err != nil {
				log.Printf("Error reading from server: %v", err)
				agent.reconnectToServer()
				continue
			}

			if messageType != websocket.BinaryMessage {
				log.Printf("Expected binary message, got type %d", messageType)
				continue
			}

			message, err := common.ReadBinaryMessage(reader)
			if err != nil {
				log.Printf("Error reading message: %v", err)
				continue
			}

			clientID := message.ClientID
			data := message.Data

			if clientID == 0 {
				// This is a control message
				err := agent.handleControlMessage(data, requestManager, clientManager)
				if err != nil {
					log.Printf("Error handling control message: %v", err)
				}
			} else {
				// Handle the message for the client
				clientManager.HandleClientMessage(clientID, data)
			}
		}
	}()
}

// handleControlMessage handles control messages received from the server
func (agent *Agent) handleControlMessage(data []byte, requestManager *RequestManager, clientManager *redisclient.ClientManager) error {
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
		return handleActivationComplete(controlMsg)
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

// StartRedisServerHeartbeats starts heartbeats for all Redis servers
func (agent *Agent) StartRedisServerHeartbeats(clientManager *redisclient.ClientManager) {
	
	clientManager.RedisServerDetailsMutex.Lock()
	defer clientManager.RedisServerDetailsMutex.Unlock()

	for redisServerID := range clientManager.RedisServerDetailsMap {
		agent.startHeartbeat(redisServerID, clientManager)
	}
}

// startHeartbeat starts a heartbeat goroutine for a specific Redis server
func (agent *Agent) startHeartbeat(redisServerID string, clientManager *redisclient.ClientManager) {
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

// stopHeartbeat stops the heartbeat for a specific Redis server
func (agent *Agent) stopHeartbeat(redisServerID string) {
	agent.heartbeatMutex.Lock()
	defer agent.heartbeatMutex.Unlock()
	if stopChan, exists := agent.heartbeatStopChans[redisServerID]; exists {
		close(stopChan)
		delete(agent.heartbeatStopChans, redisServerID)
	}
}

// performHeartbeat performs the heartbeat logic for a Redis server
func (agent *Agent) performHeartbeat(redisServerID string, clientManager *redisclient.ClientManager) {
	// Retrieve the Redis server details
	redisDetails, exists := clientManager.GetRedisServerDetails(redisServerID)

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

func (agent *Agent) SendRedisServerID(redisServerID string) error {
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
		agentID, _ := getOrCreateAgentID()
		wsConn, err := ConnectToServer(agentID, uuid.New().String())
		if err != nil { 
			log.Printf("Reconnect failed: %v", err)
			continue
		}
		agent.wsConn = wsConn
		log.Println("Reconnected to server")
		break
	}
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

func StartHTTPServer(requestManager *RequestManager, clientManager *redisclient.ClientManager, agentInstance *Agent) {
    http.HandleFunc("/activate", func(w http.ResponseWriter, r *http.Request) {
        handleActivationRequest(w, r, requestManager, clientManager, agentInstance)
    })

    log.Println("Starting HTTP server on :8081")
    err := http.ListenAndServe(":8081", nil)
    if err != nil {
        log.Fatal("HTTP server error:", err)
    }
}

func handleActivationRequest(w http.ResponseWriter, r *http.Request, requestManager *RequestManager, clientManager *redisclient.ClientManager, agentInstance *Agent) {
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

    log.Println("Received activation request with uuid:", req.UUID, "host:", req.Host, "port:", req.Port)

    // Add the Redis server details to the clientManager
    redisDetails := redisclient.RedisServerDetails{
        Host:     req.Host,
        Port:     req.Port,
        Username: req.Username,
        Password: req.Password,
    }

    clientManager.AddRedisServerDetails(req.UUID, redisDetails)

    // Save the updated Redis server details to disk
    err = clientManager.SaveRedisServerDetails(clientManager.RedisServerDetailsMap)
    if err != nil {
        log.Println("Error saving Redis server details:", err)
    }

    // Generate a unique request ID
    requestID := uuid.New().String()
    redisServerID := req.UUID

    // Create a response channel and store it in the pending requests map
    respChan := make(chan []byte)
    requestManager.AddRequest(requestID, respChan)

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
    err = agentInstance.sendControlMessage(controlMsgBytes)
    if err != nil {
        log.Println("Error sending activation control message to server:", err)
        http.Error(w, "Internal server error", http.StatusInternalServerError)
        return
    }

    // Start a heartbeat for the new Redis server
    agentInstance.startHeartbeat(redisServerID, clientManager)

    // Wait for the server's response or timeout
    select {
    case respData := <-respChan:
        // Send the server's response back to the client
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(http.StatusOK)
        w.Write(respData)
    case <-time.After(10 * time.Second):
        log.Println("Timeout waiting for server response")
        requestManager.RemoveRequest(requestID)
        http.Error(w, "Timeout waiting for server response", http.StatusGatewayTimeout)
        return
    }
}

func handleActivationComplete(controlMsg map[string]interface{}) error {
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

	requestManager.Mutex.Lock()
	respChan, exists := requestManager.PendingRequests[requestID]
	if exists {
		respChan <- responseData
		close(respChan)
		delete(requestManager.PendingRequests, requestID)
	} else {
		log.Printf("No pending request found for request_id: %s", requestID)
	}
	requestManager.Mutex.Unlock()

	return nil
}

func handleDeactivateRedisServer(controlMsg map[string]interface{}, clientManager *redisclient.ClientManager, agent *Agent) error {
	// Extract redis_server_id from control message
	redisServerID, ok := controlMsg["redis_server_id"].(string)
	if !ok {
		return fmt.Errorf("deactivate redis server message missing 'redis_server_id' field or it is not a string")
	}

	log.Printf("Received request to deactivate Redis server ID: %s", redisServerID)

	// Stop the heartbeat for the redisServerID
	agent.stopHeartbeat(redisServerID)

	// Remove the Redis server details from the clientManager
	clientManager.RemoveRedisServerDetails(redisServerID)

	// Save the updated Redis server details to disk
	err := clientManager.SaveRedisServerDetails(clientManager.RedisServerDetailsMap)
	if err != nil {
		log.Printf("Error saving Redis server details after deactivation: %v", err)
	}

	// Disconnect any clients associated with this redisServerID
	clientManager.DisconnectClientsForRedisServerID(redisServerID)

	return nil
}

// handleClientConnected handles the client connected message from the server
func handleClientConnected(controlMsg map[string]interface{}, clientManager *redisclient.ClientManager, agent *Agent) error {
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

	clientManager.AssociateClientWithRedisServerID(clientID, redisServerID)

	// Retrieve the Redis server details
	redisDetails, ok := clientManager.GetRedisServerDetails(redisServerID)

	if !ok {
		log.Printf("No Redis server details found for redis-server-id %s", redisServerID)
		return fmt.Errorf("no Redis server details found for redis-server-id %s", redisServerID)
	}

	// Build the address string
	address := net.JoinHostPort(redisDetails.Host, redisDetails.Port)

	// Create a new Redis connection for this client
	redisConn, err := net.Dial("tcp", address)
	if err != nil {
		log.Printf("Error connecting to Redis for ClientID %d at %s: %v", clientID, address, err)
		return err
	}

	client := &redisclient.RedisClient{
		ClientID:      clientID,
		RedisConn:     redisConn,
		Reader:        bufio.NewReader(redisConn),
		Writer:        bufio.NewWriter(redisConn),
		ClientManager: clientManager,
		SendMessage:   agent.sendMessage,
	}

	clientManager.StoreClient(clientID,client)

	// Start a goroutine to read responses from Redis for this client
	go client.ReadFromRedis()

	log.Printf("Client %d connected and Redis connection established", clientID)

	return nil
}

func handleClientDisconnected(controlMsg map[string]interface{}, clientManager *redisclient.ClientManager) error {
	// Extract client_id from control message
	clientIDFloat, ok := controlMsg["client_id"].(float64)
	if !ok {
		return fmt.Errorf("disconnect message missing 'client_id' field or it is not a number")
	}
	clientID := uint64(clientIDFloat)

	// Safely clean up the client's resources
	client, exists := clientManager.GetClient(clientID)

	// TODO: Do we need to remove the client from the clientManager?
	if exists {
		client.Cleanup()
		log.Printf("Client %d disconnected, cleaned up Redis connection", clientID)
	} else {
		log.Printf("Client %d not found during disconnect", clientID)
	}

	return nil
}
