package common

import (

    "encoding/binary"
    "io"
    
    "github.com/gorilla/websocket"
)

// Message represents a message with ClientID and Data
type Message struct {
    ClientID uint64
    Data     []byte
}

func ReadBinaryMessage(reader io.Reader) (Message, error) {
    var msg Message

    err := binary.Read(reader, binary.BigEndian, &msg.ClientID)
    if err != nil {
        return msg, err
    }

    var length uint32
    err = binary.Read(reader, binary.BigEndian, &length)
    if err != nil {
        return msg, err
    }

    msg.Data = make([]byte, length)
    _, err = io.ReadFull(reader, msg.Data)
    if err != nil {
        return msg, err
    }

    return msg, nil
}

func WriteBinaryMessage(wsConn *websocket.Conn, msg Message) error {
    writer, err := wsConn.NextWriter(websocket.BinaryMessage)
    if err != nil {
        return err
    }
    defer writer.Close()

    err = binary.Write(writer, binary.BigEndian, msg.ClientID)
    if err != nil {
        return err
    }

    length := uint32(len(msg.Data))
    err = binary.Write(writer, binary.BigEndian, length)
    if err != nil {
        return err
    }

    _, err = writer.Write(msg.Data)
    if err != nil {
        return err
    }

    return nil
}

