package nbmq

import (
	"encoding/json"
	"sockutils"
)

type _sender struct {
	connector   *sockutils.Connector
	readChan    chan *_message
	writeChan   chan *_message
	workflow    chan *_message
	controlflow chan *_message
	stopChan    chan struct{}
	pauseChan   chan struct{}
}

func newSender(connector *sockutils.Connector, controlflow chan *_message) *_sender {
	sender := &_sender{
		connector:   connector,
		readChan:    make(chan *_message),
		writeChan:   make(chan *_message),
		workflow:    make(chan *_message),
		controlflow: controlflow,
		stopChan:    make(chan struct{}),
		pauseChan:   make(chan struct{}),
	}
	return sender
}

func (s *_sender) write(msg *_message) {
	b, err := json.Marshal(msg)
	if err != nil {
		msg.swap()
		msg.Type = rep
		msg.Status = marshal_message_error
		msg.Data = []byte(err.Error())
		s.route(msg)
		return
	}
	s.connector.Write(b)
}

func (s *_sender) route(msg *_message) {
	msg.addArg("connector", s.connector)
	go func() {
		s.controlflow <- msg
	}()
}

func (s *_sender) put(msg *_message) {
	s.write(msg)
}

func (s *_sender) stopSender(msg *_message) {
	close(s.stopChan)
}

var senderHandlerMap = map[msgType]map[method]func(*_sender, *_message){
	ctl: map[method]func(*_sender, *_message){},
	rep: map[method]func(*_sender, *_message){
		add_sender:   (*_sender).write,
		start_sender: (*_sender).write,
		put:          (*_sender).write,
	},
	act: map[method]func(*_sender, *_message){
		put: (*_sender).put,
	},
}

func (s *_sender) handle(msg *_message) {
	h := senderHandlerMap[msg.Type][msg.Method]
	if h == nil {
		return
	}
	h(s, msg)
}

func (s *_sender) run() {
	for {
		select {
		case <-s.connector.Done():
			ctlMsg := newMessage(ctl, sender, group, remove_sender, undefined_status)
			s.route(ctlMsg)
			close(s.workflow)
			return
		case <-s.stopChan:
			return
		case <-s.pauseChan:
			<-s.pauseChan
		case b, ok := <-s.connector.ReadChan():
			if !ok {
				continue
			}
			var msg _message
			err := json.Unmarshal(b, &msg)
			if err != nil {
				msg.swap()
				msg.Type = rep
				msg.Status = unmarshal_message_error
				msg.Data = []byte(err.Error())
				s.route(&msg)
				continue
			}
			if msg.Destination == sender {
				s.handle(&msg)
			} else {
				s.route(&msg)
			}
		case msg := <-s.workflow:
			if msg.Destination == sender {
				s.handle(msg)
			} else {
				continue
			}
		}
	}
}
