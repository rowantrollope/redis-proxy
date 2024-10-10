package redisclient

import (
    "bufio"
    "net"
    "sync"
	"log"
    "io"
    
    "github.com/rowantrollope/redis-proxy/agent/common"
)

type SendMessageFunc func(msg common.Message) error

type RedisClient struct {
    ClientID      uint64
    RedisConn     net.Conn
    Reader        *bufio.Reader
    Writer        *bufio.Writer
    cleanupOnce   sync.Once
    ClientManager *ClientManager
    SendMessage   SendMessageFunc
}

func NewRedisClient(clientID uint64, conn net.Conn, clientManager *ClientManager) *RedisClient {
    return &RedisClient{
        ClientID:      clientID,
        RedisConn:     conn,
        Reader:        bufio.NewReader(conn),
        Writer:        bufio.NewWriter(conn),
        ClientManager: clientManager,
    }
}

func (client *RedisClient) WriteToRedis(data []byte) error {
    _, err := client.Writer.Write(data)
    if err != nil {
        return err
    }
    return client.Writer.Flush()
}

func (client *RedisClient) ReadFromRedis() {
    defer client.Cleanup()

    for {
        response, err := client.readRedisResponse(client.Reader)
        if err != nil {
            if err == io.EOF {
                log.Printf("Redis connection closed for clientID %d", client.ClientID)
            } else {
                log.Printf("Error reading from Redis for clientID %d: %v", client.ClientID, err)
            }
            return
        }

        msg := common.Message{
            ClientID: client.ClientID,
            Data:     response,
        }

        err = client.SendMessage(msg)
        if err != nil {
            log.Printf("Error writing to server for clientID %d: %v", client.ClientID, err)
            return
        }
    }
}


func (client *RedisClient) readRedisResponse(reader *bufio.Reader) ([]byte, error) {
    var response []byte
    for {
        line, err := reader.ReadBytes('\n')
        if err != nil {
            return nil, err
        }
        response = append(response, line...)
        if client.isCompleteRedisResponse(response) {
            break
        }
    }
    return response, nil
}

func (client *RedisClient) isCompleteRedisResponse(response []byte) bool {
    // Implement proper RESP protocol parsing
    // For simplicity, assume every line is a complete response
    return true
}

func (client *RedisClient) Cleanup() {
	client.cleanupOnce.Do(func() {
		// Close the Redis connection
		client.RedisConn.Close()

		// Remove the client from clients map
		client.ClientManager.clientsMutex.Lock()
		delete(client.ClientManager.clients, client.ClientID)
		client.ClientManager.clientsMutex.Unlock()

		// Remove the clientID mapping from clientRedisServerIDMap
		client.ClientManager.clientRedisServerIDMutex.Lock()
		delete(client.ClientManager.clientRedisServerIDMap, client.ClientID)
		client.ClientManager.clientRedisServerIDMutex.Unlock()

		log.Printf("Cleaned up Redis client for ClientID %d", client.ClientID)
	})
}

// Method to read from Redis and send messages back to the agent...