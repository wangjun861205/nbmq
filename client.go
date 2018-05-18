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

func (c *_client) stop() {
	close(c.stopChan)
}

func (c *_client) write(msg *_message) {
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				msg.swap()
				msg.Type = rep
				msg.Status = connector_write_error
				c.route(msg)
			}
		}()
		c.connector.writer.msgChan <- msg
	}()
}

func (c *_client) handleAddReceiverRep(msg *_message) {
	if msg.Status != success {
		c.write(msg)
		return
	}
	c.stop()
	removeClientMsg := newMessage(ctl, client, listener, remove_client, undefined_status)
	c.route(removeClientMsg)
	startReceiverMsg := newMessage(ctl, client, queue, start_receiver, undefined_status)
	startReceiverMsg.addArg("topic", msg.getArg("topic"))
	c.route(startReceiverMsg)
}

func (c *_client) handleAddSenderRep(msg *_message) {
	if msg.Status != success {
		c.write(msg)
		return
	}
	c.stop()
	removeClientMsg := newMessage(ctl, client, listener, remove_client, undefined_status)
	c.route(removeClientMsg)
	startSenderMsg := newMessage(ctl, client, group, start_sender, undefined_status)
	startSenderMsg.addArg("topic", msg.getArg("topic"))
	startSenderMsg.addArg("group", msg.getArg("group"))
	c.route(startSenderMsg)
}

var clientHandlerMap = map[msgType]map[method]func(*_client, *_message){
	ctl: map[method]func(*_client, *_message){},
	rep: map[method]func(*_client, *_message){
		add_receiver: (*_client).handleAddReceiverRep,
		add_sender:   (*_client).handleAddSenderRep,
		queues_info:  (*_client).write,
		add_queue:    (*_client).write,
		add_group:    (*_client).write,
		put:          (*_client).write,
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
