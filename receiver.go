package nbmq

type _receiver struct {
	connector   *_connector
	workflow    chan *_message
	controlflow chan *_message
	stopChan    chan struct{}
	pauseChan   chan struct{}
}

func newReceiver(connector *_connector, workflow chan *_message) *_receiver {
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
}

func (r *_receiver) route(msg *_message) {
	msg.addArg("connector", r.connector)
	go func() {
		r.workflow <- msg
	}()
}

func (r *_receiver) stopReceiver(msg *_message) {
	r.stop()
}

func (r *_receiver) write(msg *_message) {
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				msg.swap()
				msg.Type = rep
				msg.Status = connector_write_error
				r.route(msg)
			}
		}()
		r.connector.writer.msgChan <- msg
	}()
}

var receiverHandlerMap = map[msgType]map[method]func(*_receiver, *_message){
	ctl: map[method]func(*_receiver, *_message){
		stop_receiver: (*_receiver).stopReceiver,
	},
	rep: map[method]func(*_receiver, *_message){
		add_receiver: (*_receiver).write,
		put:          (*_receiver).write,
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
		case <-r.connector.done:
			ctlMsg := newMessage(ctl, receiver, queue, remove_receiver, undefined_status)
			r.route(ctlMsg)
			return
		case <-r.stopChan:
			return
		case <-r.pauseChan:
			<-r.pauseChan
		case msg := <-r.connector.reader.msgChan:
			if msg.Destination == receiver {
				r.handle(msg)
			} else {
				r.route(msg)
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
