package nbmq

import (
	"encoding/json"
	"fmt"
	"sockutils"
)

type _receiver struct {
	connector   *sockutils.Connector
	workflow    chan *_message
	controlflow chan *_message
	stopChan    chan struct{}
	pauseChan   chan struct{}
}

func newReceiver(connector *sockutils.Connector, workflow chan *_message) *_receiver {
	receiver := &_receiver{
		connector:   connector,
		workflow:    workflow,
		controlflow: make(chan *_message),
		stopChan:    make(chan struct{}),
		pauseChan:   make(chan struct{}),
	}
	return receiver
}

func (r *_receiver) stop() {
	close(r.stopChan)
}

func (r *_receiver) pause() {
	r.pauseChan <- struct{}{}
}

func (r *_receiver) role() role {
	return receiver
}

func (r *_receiver) put(msg *_message) {
	msg.addArg("connector", r.connector)
	msg.Destination = queue
	r.route(msg)
	fmt.Println("=========================")
}

func (r *_receiver) route(msg *_message) {
	msg.addArg("connector", r.connector)
	go func() {
		r.workflow <- msg
	}()
}

func (r *_receiver) write(msg *_message) {
	b, err := json.Marshal(msg)
	if err != nil {
		msg.swap()
		msg.Type = rep
		msg.Status = marshal_message_error
		msg.Data = []byte(err.Error())
		r.route(msg)
		return
	}
	r.connector.Write(b)
}

var receiverHandlerMap = map[msgType]map[method]func(*_receiver, *_message){
	ctl: map[method]func(*_receiver, *_message){},
	rep: map[method]func(*_receiver, *_message){
		add_queue:      (*_receiver).write,
		add_receiver:   (*_receiver).write,
		start_receiver: (*_receiver).write,
		put:            (*_receiver).write,
	},
	act: map[method]func(*_receiver, *_message){
		put: (*_receiver).put,
	},
}

func (r *_receiver) handle(msg *_message) {
	h := receiverHandlerMap[msg.Type][msg.Method]
	if h == nil {
		return
	}
	h(r, msg)
}

func (r *_receiver) run() {
	for {
		select {
		case <-r.connector.Done():
			ctlMsg := newMessage(ctl, receiver, queue, remove_receiver, undefined_status)
			close(r.controlflow)
			r.route(ctlMsg)
			return
		case <-r.stopChan:
			return
		case <-r.pauseChan:
			<-r.pauseChan
		case b, ok := <-r.connector.ReadChan():
			if !ok {
				continue
			}
			var msg _message
			err := json.Unmarshal(b, &msg)
			if err != nil {
				msg.swap()
				msg.Type = rep
				msg.Source = receiver
				msg.Status = unmarshal_message_error
				msg.Data = []byte(err.Error())
				r.route(&msg)
				continue
			}
			if msg.Destination == receiver {
				r.handle(&msg)
			} else {
				r.route(&msg)
			}
		case msg := <-r.controlflow:
			if msg.Destination == receiver {
				r.handle(msg)
			} else {
				continue
			}
		}
	}
}
