package nbmq

import (
	"encoding/json"
	"log"
	"net"
	"os"
	"sockutils"
)

type _listener struct {
	listener    net.Listener
	queues      map[string]*_queue
	clients     map[*sockutils.Connector]*_client
	workflow    chan *_message
	controlflow chan *_message
	stopChan    chan struct{}
	pauseChan   chan struct{}
	logger      *log.Logger
}

func NewListener(addr string) (*_listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	logger := log.New(os.Stdout, "listener", log.Ltime)
	return &_listener{
		listener:    l,
		queues:      make(map[string]*_queue),
		clients:     make(map[*sockutils.Connector]*_client),
		workflow:    make(chan *_message),
		controlflow: make(chan *_message),
		stopChan:    make(chan struct{}),
		pauseChan:   make(chan struct{}),
		logger:      logger,
	}, nil
}

func (l *_listener) Stop() {
	close(l.stopChan)
}

func (l *_listener) addQueue(msg *_message) {
	if msg.Source != client {
		msg.swap()
		msg.Type = rep
		msg.Status = role_error
		l.route(msg)
		return
	}
	topic := msg.getArg("topic").(string)
	if _, ok := l.queues[topic]; ok {
		msg.Type = rep
		msg.swap()
		msg.Status = topic_exists_error
		l.route(msg)
		return
	} else {
		queue := newQueue(topic, l)
		l.queues[topic] = queue
		msg.Type = rep
		msg.swap()
		msg.Status = success
		l.route(msg)
	}
}

func (l *_listener) removeQueue(msg *_message) {
	topic := msg.getArg("topic").(string)
	delete(l.queues, topic)
}

func (l *_listener) listen() {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			continue
		}
		tcpConn := conn.(*net.TCPConn)
		tcpConn.SetKeepAlive(true)
		c := newClient(tcpConn, l.workflow)
		l.clients[c.connector] = c
	}
}

func (l *_listener) routeToClient(msg *_message) {
	connector := msg.getArg("connector").(*sockutils.Connector)
	if c, ok := l.clients[connector]; !ok {
		msg.swap()
		msg.Type = rep
		msg.Status = client_not_exists_error
		l.route(msg)
	} else {
		go func() {
			defer recover()
			c.controlflow <- msg
		}()
	}
}

func (l *_listener) routeToQueue(msg *_message) {
	if t := msg.getArg("topic"); t == nil {
		msg.swap()
		msg.Type = rep
		msg.Source = listener
		msg.Status = topic_arg_error
		l.route(msg)
		return
	} else {
		topic := t.(string)
		if q, ok := l.queues[topic]; !ok {
			msg.swap()
			msg.Type = rep
			msg.Source = listener
			msg.Status = queue_not_exists_error
			l.route(msg)
		} else {
			go func() {
				q.workflow <- msg
			}()
		}
	}
}

func (l *_listener) route(msg *_message) {
	switch msg.Destination {
	case client:
		l.routeToClient(msg)
	default:
		l.routeToQueue(msg)
	}
}

func (l *_listener) stopAndRemoveClient(msg *_message) {
	connector := msg.getArg("connector").(*sockutils.Connector)
	close(l.clients[connector].stopChan)
	delete(l.clients, connector)
	msg.swap()
	msg.Type = rep
	msg.Status = success
	l.route(msg)
}

func (l *_listener) removeClient(msg *_message) {
	connector := msg.getArg("connector").(*sockutils.Connector)
	delete(l.clients, connector)
}

func (l *_listener) queuesInfo(msg *_message) {
	topics := make([]string, 0, 16)
	for _, q := range l.queues {
		topics = append(topics, q.topic)
	}
	b, _ := json.Marshal(topics)
	msg.Type = rep
	msg.Data = b
	msg.swap()
	l.route(msg)
}

var listenerHandlerMap = map[msgType]map[method]func(*_listener, *_message){
	ctl: map[method]func(*_listener, *_message){
		add_queue:              (*_listener).addQueue,
		remove_queue:           (*_listener).removeQueue,
		remove_client:          (*_listener).removeClient,
		queues_info:            (*_listener).queuesInfo,
		stop_and_remove_client: (*_listener).stopAndRemoveClient,
	},
	rep: map[method]func(*_listener, *_message){},
	act: map[method]func(*_listener, *_message){},
}

func (l *_listener) handle(msg *_message) {
	h := listenerHandlerMap[msg.Type][msg.Method]
	if h == nil {
		return
	}
	h(l, msg)
}

func (l *_listener) Run() {
	go l.listen()
	for {
		select {
		case <-l.stopChan:
			return
		case <-l.pauseChan:
			<-l.pauseChan
			continue
		case msg := <-l.workflow:
			if msg.Destination == listener {
				l.handle(msg)
			} else {
				l.route(msg)
			}
		case msg := <-l.controlflow:
			if msg.Destination == listener {
				l.handle(msg)
			} else {
				l.route(msg)
			}
		}
	}
}
