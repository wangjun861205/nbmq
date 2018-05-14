package nbmq

import (
	"fmt"
	"net"
)

type Client struct {
	connector *_connector
	role      role
	handler   func([]byte)
}

func NewClient(addr string, handler func([]byte)) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(true)
	connector := newConnector(tcpConn)
	client := &Client{connector, client, handler}
	go client.run()
	return client, nil
}

func (c *Client) AddQueue(topic string) {
	if c.role != client {
		fmt.Println("only client can add queue")
		return
	}
	msg := newMessage(ctl, client, listener, add_queue, undefined_status)
	msg.addArg("topic", topic)
	go func() {
		c.connector.writer.msgChan <- msg
	}()
}

func (c *Client) QueuesInfo() {
	msg := newMessage(ctl, client, listener, queues_info, undefined_status)
	go func() {
		c.connector.writer.msgChan <- msg
	}()
}

func (c *Client) AddReceiver(topic string) {
	if c.role != client {
		fmt.Println("only client can add receiver")
		return
	}
	msg := newMessage(ctl, client, queue, add_receiver, undefined_status)
	msg.addArg("topic", topic)
	go func() {
		c.connector.writer.msgChan <- msg
	}()
}

func (c *Client) AddGroup(topic, groupName string) {
	if c.role != client {
		fmt.Println("only client can add group")
		return
	}
	msg := newMessage(ctl, client, queue, add_group, undefined_status)
	msg.addArg("topic", topic)
	msg.addArg("group", groupName)
	go func() {
		c.connector.writer.msgChan <- msg
	}()
}

func (c *Client) AddSender(topic, groupName string) {
	if c.role != client {
		fmt.Println("only client can add group")
		return
	}
	msg := newMessage(ctl, client, group, add_sender, undefined_status)
	msg.addArg("topic", topic)
	msg.addArg("group", groupName)
	go func() {
		c.connector.writer.msgChan <- msg
	}()
}

func (c *Client) Put(data []byte) {
	if c.role != receiver {
		fmt.Println("only receiver can put message")
	}
	msg := newMessage(act, receiver, queue, put, undefined_status)
	msg.Data = data
	go func() {
		c.connector.writer.msgChan <- msg
	}()
}

func (c *Client) Close() {
	c.connector.reader.conn.Close()
}

func (c *Client) handleMsg(msg *_message) {
	if c.handler != nil {
		go func() {
			c.handler(msg.Data)
		}()
	}
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

var mqclientHandlerMap = map[msgType]map[method]func(*Client, *_message){
	ctl: map[method]func(*Client, *_message){},
	rep: map[method]func(*Client, *_message){
		add_receiver: (*Client).addReceiverRep,
		add_sender:   (*Client).addSenderRep,
		add_queue:    (*Client).addQueueRep,
		add_group:    (*Client).addGroupRep,
		queues_info:  (*Client).queuesInfoRep,
		put:          (*Client).putRep,
	},
	act: map[method]func(*Client, *_message){
		put: (*Client).handleMsg,
	},
}

func (c *Client) handle(msg *_message) {
	h := mqclientHandlerMap[msg.Type][msg.Method]
	if h == nil {
		return
	}
	h(c, msg)
}

func (c *Client) run() {
	for {
		select {
		case <-c.connector.done:
			return
		case msg := <-c.connector.reader.msgChan:
			c.handle(msg)
		}
	}
}
