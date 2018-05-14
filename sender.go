package nbmq

type _sender struct {
	connector   *_connector
	readChan    chan *_message
	writeChan   chan *_message
	workflow    chan *_message
	controlflow chan *_message
	stopChan    chan struct{}
	pauseChan   chan struct{}
}

func newSender(connector *_connector, controlflow chan *_message) *_sender {
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
	go func() {
		s.connector.writer.msgChan <- msg
	}()
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
		add_sender: (*_sender).write,
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
		case <-s.connector.done:
			ctlMsg := newMessage(ctl, sender, group, remove_sender, undefined_status)
			s.route(ctlMsg)
			return
		case <-s.stopChan:
			return
		case <-s.pauseChan:
			<-s.pauseChan
		case msg := <-s.connector.reader.msgChan:
			if msg.Destination == sender {
				s.handle(msg)
			} else {
				s.route(msg)
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
