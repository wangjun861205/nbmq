package nbmq

type _queue struct {
	topic       string
	receivers   map[*_connector]*_receiver
	groups      map[string]*_group
	listener    *_listener
	workflow    chan *_message
	controlflow chan *_message
	stopChan    chan struct{}
	pauseChan   chan struct{}
}

func newQueue(topic string, listener *_listener) *_queue {
	queue := &_queue{
		topic:       topic,
		receivers:   make(map[*_connector]*_receiver),
		groups:      make(map[string]*_group),
		listener:    listener,
		workflow:    make(chan *_message),
		controlflow: make(chan *_message),
		stopChan:    make(chan struct{}),
		pauseChan:   make(chan struct{}),
	}
	go queue.run()
	return queue
}

func (q *_queue) stop() {
	close(q.stopChan)
}

func (q *_queue) pause() {
	q.pauseChan <- struct{}{}
}

func (q *_queue) role() role {
	return queue
}

func (q *_queue) broadcastToReceivers(msg *_message) {
	msg.addArg("topic", q.topic)
	for _, r := range q.receivers {
		go func() {
			r.controlflow <- msg.copy()
		}()
	}
}

func (q *_queue) broadcastToGroups(msg *_message) {
	msg.addArg("topic", q.topic)
	for _, g := range q.groups {
		go func() {
			g.workflow <- msg.copy()
		}()
	}
}

func (q *_queue) broadcast(msg *_message) {
	switch msg.Destination {
	case receiver:
		q.broadcastToReceivers(msg)
	case group, sender:
		q.broadcastToGroups(msg)
	}
}

func (q *_queue) removeQueue(msg *_message) {
	recMsg := newMessage(ctl, queue, receiver, stop_receiver, undefined_status)
	q.broadcast(recMsg)
	grpMsg := newMessage(ctl, queue, group, stop_receiver, undefined_status)
	q.broadcast(grpMsg)
}

func (q *_queue) removeGroup(msg *_message) {
	groupName := msg.getArg("group").(string)
	delete(q.groups, groupName)
}

func (q *_queue) addGroup(msg *_message) {
	groupName := msg.getArg("group").(string)
	if _, ok := q.groups[groupName]; ok {
		msg.swap()
		msg.Type = rep
		msg.Status = group_exists_error
		q.route(msg)
	} else {
		q.groups[groupName] = newGroup(q, groupName)
		msg.swap()
		msg.Type = rep
		msg.Status = success
		q.route(msg)
	}
}

func (q *_queue) removeReceiver(msg *_message) {
	connector := msg.getArg("connector").(*_connector)
	delete(q.receivers, connector)
}

func (q *_queue) addReceiver(msg *_message) {
	connector := msg.getArg("connector").(*_connector)
	if _, ok := q.receivers[connector]; ok {
		msg.swap()
		msg.Type = rep
		msg.Status = receiver_exists_error
		q.route(msg)
		return
	}
	rec := newReceiver(connector, q.workflow)
	q.receivers[connector] = rec
	ctlMsg := newMessage(ctl, queue, listener, stop_and_remove_client, undefined_status)
	ctlMsg.addArg("connector", connector)
	q.route(ctlMsg)
}

func (q *_queue) startReceiver(msg *_message) {
	if msg.Status == success {
		connector := msg.getArg("connector").(*_connector)
		go q.receivers[connector].run()
		repMsg := newMessage(rep, queue, receiver, add_receiver, success)
		repMsg.addArg("connector", connector)
		q.route(repMsg)
	}
}

func (q *_queue) addSender(msg *_message) {
	groupName := msg.getArg("group").(string)
	if _, ok := q.groups[groupName]; !ok {
		msg.swap()
		msg.Type = rep
		msg.Source = queue
		msg.Status = no_group_error
		q.route(msg)
		return
	}
	msg.Destination = group
	q.route(msg)
}

func (q *_queue) routeToGroup(msg *_message) {
	groupName := msg.getArg("group").(string)
	if g, ok := q.groups[groupName]; !ok {
		msg.swap()
		msg.Type = rep
		msg.Source = queue
		msg.Status = group_not_exists_error
		q.route(msg)
	} else {
		go func() {
			g.workflow <- msg
		}()
	}
}

func (q *_queue) routeToReceiver(msg *_message) {
	connector := msg.getArg("connector").(*_connector)
	if r, ok := q.receivers[connector]; !ok {
		msg.swap()
		msg.Source = queue
		msg.Status = receiver_not_exists_error
		q.route(msg)
	} else {
		go func() {
			r.controlflow <- msg
		}()
	}
}

func (q *_queue) routeToListener(msg *_message) {
	go func() {
		q.listener.controlflow <- msg
	}()
}

func (q *_queue) route(msg *_message) {
	msg.addArg("topic", q.topic)
	switch msg.Destination {
	case receiver:
		q.routeToReceiver(msg)
	case listener, client:
		q.routeToListener(msg)
	default:
		q.routeToGroup(msg)
	}
}

var queueHandlerMap = map[msgType]map[method]func(*_queue, *_message){
	ctl: map[method]func(*_queue, *_message){
		add_receiver:    (*_queue).addReceiver,
		add_group:       (*_queue).addGroup,
		add_sender:      (*_queue).addSender,
		remove_receiver: (*_queue).removeReceiver,
	},
	rep: map[method]func(*_queue, *_message){
		stop_and_remove_client: (*_queue).startReceiver,
	},
	act: map[method]func(*_queue, *_message){
		put: (*_queue).put,
	},
}

func (q *_queue) handle(msg *_message) {
	h := queueHandlerMap[msg.Type][msg.Method]
	if h == nil {
		return
	}
	h(q, msg)
}

func (q *_queue) put(msg *_message) {
	msg.addArg("topic", q.topic)
	if len(q.groups) == 0 {
		msg.swap()
		msg.Type = rep
		msg.Status = no_group_error
		q.route(msg)
		return
	}
	msg.Destination = group
	for _, g := range q.groups {
		dupMsg := msg.copy()
		go func(g *_group) {
			g.workflow <- dupMsg
		}(g)
	}
}

func (q *_queue) run() {
	for {
		select {
		case <-q.stopChan:
			return
		case <-q.pauseChan:
			<-q.pauseChan
			continue
		case msg := <-q.listener.workflow:
			if msg.Destination == queue {
				q.handle(msg)
			} else {
				q.route(msg)
			}
		case msg := <-q.workflow:
			if msg.Destination == queue {
				q.handle(msg)
			} else {
				q.route(msg)
			}
		case msg := <-q.controlflow:
			if msg.Destination == queue {
				q.handle(msg)
			} else {
				q.route(msg)
			}
		}
	}
}
