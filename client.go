package nbmq

import (
	"net"
)

type _client struct {
	connector   *_connector
	workflow    chan *_message
	controlflow chan *_message
	stopChan    chan struct{}
	pauseChan   chan struct{}
}

func newClient(conn *net.TCPConn, workflow chan *_message) *_client {
	connector := newConnector(conn)
	client := &_client{
		connector:   connector,
		workflow:    workflow,
		controlflow: make(chan *_message),
		stopChan:    make(chan struct{}),
		pauseChan:   make(chan struct{}),
	}
	go client.run()
	return client
}

func (c *_client) route(msg *_message) {
	msg.addArg("connector", c.connector)
	go func() {
		c.workflow <- msg
	}()
}

func (c *_client) stopClient(msg *_message) {
	close(c.stopChan)
}

func (c *_client) write(msg *_message) {
	go func() {
		c.connector.writer.msgChan <- msg
	}()
}

var clientHandlerMap = map[msgType]map[method]func(*_client, *_message){
	ctl: map[method]func(*_client, *_message){},
	rep: map[method]func(*_client, *_message){
		queues_info:  (*_client).write,
		add_queue:    (*_client).write,
		add_receiver: (*_client).write,
		add_group:    (*_client).write,
		add_sender:   (*_client).write,
	},
	act: map[method]func(*_client, *_message){},
}

func (c *_client) handle(msg *_message) {
	h := clientHandlerMap[msg.Type][msg.Method]
	if h == nil {
		return
	}
	h(c, msg)
}

func (c *_client) run() {
	for {
		select {
		case <-c.connector.done:
			ctlMsg := newMessage(ctl, client, listener, remove_client, undefined_status)
			ctlMsg.addArg("connector", c.connector)
			c.route(ctlMsg)
			return
		case <-c.stopChan:
			return
		case <-c.pauseChan:
			<-c.pauseChan
		case msg := <-c.controlflow:
			if msg.Destination == client {
				c.handle(msg)
			} else {
				continue
			}
		case msg := <-c.connector.reader.msgChan:
			if msg.Destination == client {
				c.handle(msg)
			} else {
				c.route(msg)
			}
		}
	}
}
