package agent

import "sync"

type RequestManager struct {
    PendingRequests map[string]chan []byte
    Mutex           sync.Mutex
}

func NewRequestManager() *RequestManager {
    return &RequestManager{
        PendingRequests: make(map[string]chan []byte),
    }
}

func (rm *RequestManager) AddRequest(requestID string, responseChan chan []byte) {
    rm.Mutex.Lock()
    defer rm.Mutex.Unlock()
    rm.PendingRequests[requestID] = responseChan
}

func (rm *RequestManager) RemoveRequest(requestID string) {
    rm.Mutex.Lock()
    defer rm.Mutex.Unlock()
    delete(rm.PendingRequests, requestID)
}

// Additional methods as needed...