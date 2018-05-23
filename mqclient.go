package nbmq

import (
	"encoding/json"
	"fmt"
	"net"
	"sockutils"
)

type Client struct {
	connector *sockutils.Connector
	role      role
	dataChan  chan []byte
	done      chan struct{}
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(true)
	tcpConn.SetNoDelay(true)
	connector := sockutils.NewConnector(tcpConn, "\r\n\r\n")
	client := &Client{connector, client, make(chan []byte), make(chan struct{})}
	go client.run()
	return client, nil
}

func (c *Client) write(msg *_message) {
	b, err := json.Marshal(msg)
	if err != nil {
		msg.Type = rep
		msg.Status = marshal_message_error
		msg.Data = []byte(err.Error())
		b, _ := json.Marshal(msg)
		c.connector.Write(b)
		return
	}
	c.connector.Write(b)
}

func (c *Client) AddQueue(topic string) {
	msg := newMessage(ctl, c.role, listener, add_queue, undefined_status)
	msg.addArg("topic", topic)
	c.write(msg)
}

func (c *Client) QueuesInfo() {
	msg := newMessage(ctl, client, listener, queues_info, undefined_status)
	c.write(msg)
}

func (c *Client) AddReceiver(topic string) {
	msg := newMessage(ctl, c.role, queue, add_receiver, undefined_status)
	msg.addArg("topic", topic)
	c.write(msg)
}

func (c *Client) handleAddReceiverRep(msg *_message) {
	fmt.Println(msg)
}

func (c *Client) handleStartReceiverRep(msg *_message) {
	if msg.Status == success {
		c.role = receiver
	}
	fmt.Println(msg)
}

func (c *Client) AddGroup(topic, groupName string) {
	if c.role != client {
		fmt.Println("only client can add group")
		return
	}
	msg := newMessage(ctl, client, queue, add_group, undefined_status)
	msg.addArg("topic", topic)
	msg.addArg("group", groupName)
	c.write(msg)
}

func (c *Client) AddSender(topic, groupName string) {
	msg := newMessage(ctl, c.role, group, add_sender, undefined_status)
	msg.addArg("topic", topic)
	msg.addArg("group", groupName)
	c.write(msg)
}

func (c *Client) handleAddSenderRep(msg *_message) {
	fmt.Println(msg)
}

func (c *Client) handleStartSenderRep(msg *_message) {
	if msg.Status == success {
		c.role = sender
	}
	fmt.Println(msg)
}

func (c *Client) Put(data []byte) {
	msg := newMessage(act, c.role, queue, put, undefined_status)
	msg.Data = data
	c.write(msg)
}

func (c *Client) Close() {
	c.connector.Close()
}

func (c *Client) handlPutMsg(msg *_message) {
	go func() {
		defer recover()
		c.dataChan <- msg.Data
	}()
}

func (c *Client) addGroupRep(msg *_message) {
	fmt.Println(msg)
}

func (c *Client) addReceiverRep(msg *_message) {
	if msg.Status == success {
		c.role = receiver
	}
	fmt.Println(msg)
}

func (c *Client) addSenderRep(msg *_message) {
	if msg.Status == success {
		c.role = sender
	}
	fmt.Println(msg)
}

func (c *Client) addQueueRep(msg *_message) {
	fmt.Println(msg)
}

func (c *Client) queuesInfoRep(msg *_message) {
	fmt.Println(msg)
}

func (c *Client) putRep(msg *_message) {
	fmt.Println(msg)
}

func (c *Client) Done() chan struct{} {
	return c.done
}

var mqclientHandlerMap = map[msgType]map[method]func(*Client, *_message){
	ctl: map[method]func(*Client, *_message){},
	rep: map[method]func(*Client, *_message){
		add_receiver:   (*Client).handleAddReceiverRep,
		start_receiver: (*Client).handleStartReceiverRep,
		add_sender:     (*Client).handleAddSenderRep,
		start_sender:   (*Client).handleStartSenderRep,
		add_queue:      (*Client).addQueueRep,
		add_group:      (*Client).addGroupRep,
		queues_info:    (*Client).queuesInfoRep,
		put:            (*Client).putRep,
	},
	act: map[method]func(*Client, *_message){
		put: (*Client).handlPutMsg,
	},
}

func (c *Client) handle(msg *_message) {
	h := mqclientHandlerMap[msg.Type][msg.Method]
	if h == nil {
		return
	}
	h(c, msg)
}

func (c *Client) DataChan() chan []byte {
	return c.dataChan
}

func (c *Client) run() {
	for {
		select {
		case <-c.connector.Done():
			fmt.Println("connection has closed")
			close(c.dataChan)
			close(c.done)
			return
		case b, ok := <-c.connector.ReadChan():
			if !ok {
				continue
			}
			var msg _message
			err := json.Unmarshal(b, &msg)
			if err != nil {
				msg.Type = rep
				msg.Status = unmarshal_message_error
				msg.Data = []byte(err.Error())
				b, _ := json.Marshal(msg)
				c.connector.Write(b)
				continue
			}
			c.handle(&msg)
		}
	}
}
