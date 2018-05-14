package nbmq

import (
	"encoding/json"
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
	for {
		b, err := readConn(r.conn)
		if err != nil {
			close(r.done)
			r.conn.Close()
			return
		}
		if msg, ok := unmarshalMessage(b); !ok {
			b, _ = json.Marshal(msg)
			if _, err := r.conn.Write(b); err != nil {
				close(r.done)
				return
			}
		} else {
			go func() {
				r.msgChan <- msg
			}()
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
		b, _ := json.Marshal(msg)
		b = append(b, []byte("\r\n\r\n")...)
		_, err := w.conn.Write(b)
		if err != nil {
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
