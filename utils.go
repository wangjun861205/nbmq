package nbmq

import (
	"bytes"
	"encoding/json"
	"net"
)

func writeResp(conn *net.TCPConn, status status, message ...string) error {
	buffer := bytes.NewBuffer(make([]byte, 0, 256))
	buffer.WriteString(string(status) + "\r\n")
	for _, m := range message {
		buffer.WriteString(m + "\r\n")
	}
	buffer.WriteString("\r\n")
	_, err := conn.Write(buffer.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func readConn(conn *net.TCPConn) ([]byte, error) {
	buffer := make([]byte, 256)
	content := make([]byte, 0, 4096)
	for {
		i, err := conn.Read(buffer)
		if err != nil {
			return nil, err
		}
		content = append(content, buffer[:i]...)
		if bytes.Contains(content, []byte("\r\n\r\n")) {
			break
		}
	}
	content = content[:len(content)-4]
	return content, nil
}

func writeConn(conn *net.TCPConn, message *_message) error {
	b, err := json.Marshal(message)
	if err != nil {
		return err
	}
	b = append(b, []byte("\r\n\r\n")...)
	_, err = conn.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func unmarshalMessage(content []byte) (*_message, bool) {
	var msg _message
	err := json.Unmarshal(content, &msg)
	if err != nil {
		msg.Type = rep
		msg.Source = reader
		msg.Status = unmarshal_message_error
		msg.Data = []byte(err.Error())
		return &msg, false
	}
	return &msg, true
}

func marshalMessage(msg *_message) ([]byte, bool) {
	b, err := json.Marshal(msg)
	if err != nil {
		msg.swap()
		msg.Type = rep
		msg.Status = marshal_message_error
		return nil, false
	}
	return b, true
}
