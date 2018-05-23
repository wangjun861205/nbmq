package nbmq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
)

type _reader struct {
	conn    *net.TCPConn
	msgChan chan *_message
	done    chan struct{}
}

func newReader(conn *net.TCPConn) *_reader {
	reader := &_reader{
		conn:    conn,
		msgChan: make(chan *_message),
		done:    make(chan struct{}),
	}
	go reader.run()
	return reader
}

func (r *_reader) run() {
	buffer := make([]byte, 1024)
	content := make([]byte, 0, 4096)
	var msgContent []byte
	for {
		n, err := r.conn.Read(buffer)
		if err != nil {
			close(r.done)
			r.conn.Close()
			return
		}
		content = append(content, buffer[:n]...)
		if i := bytes.Index(content, []byte("\r\n\r\n")); i != -1 {
			var msg _message
			msgContent, content = content[:i], content[i+4:]
			err := json.Unmarshal(msgContent, &msg)
			if err != nil {
				fmt.Println(err)
				continue
			}
			go func(msg *_message) {
				r.msgChan <- msg
			}(&msg)
		}
	}
}

type _writer struct {
	conn    *net.TCPConn
	msgChan chan *_message
	done    chan struct{}
}

func newWriter(conn *net.TCPConn) *_writer {
	writer := &_writer{conn, make(chan *_message), make(chan struct{})}
	go writer.run()
	return writer
}

func (w *_writer) run() {
	for {
		msg, ok := <-w.msgChan
		if !ok {
			close(w.done)
			return
		}
		b, err := json.Marshal(msg)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		b = append(b, []byte("\r\n\r\n")...)
		_, err = w.conn.Write(b)
		if err != nil {
			fmt.Println(err)
			close(w.done)
			return
		}
	}
}

type _connector struct {
	reader *_reader
	writer *_writer
	done   chan struct{}
}

func newConnector(conn *net.TCPConn) *_connector {
	connector := &_connector{newReader(conn), newWriter(conn), make(chan struct{})}
	go connector.run()
	return connector
}

func (c *_connector) run() {
	<-c.reader.done
	close(c.writer.msgChan)
	<-c.writer.done
	close(c.done)
}
