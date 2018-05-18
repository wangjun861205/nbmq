package nbmq

type _group struct {
	name        string
	queue       *_queue
	numSender   int64
	index       uint64
	putList     []*_sender
	senders     map[*_connector]*_sender
	workflow    chan *_message
	controlflow chan *_message
	stopChan    chan struct{}
	pauseChan   chan struct{}
}

func newGroup(queue *_queue, name string) *_group {
	group := &_group{
		name:        name,
		queue:       queue,
		putList:     make([]*_sender, 0, 64),
		senders:     make(map[*_connector]*_sender),
		workflow:    make(chan *_message),
		controlflow: make(chan *_message),
		stopChan:    make(chan struct{}),
		pauseChan:   make(chan struct{}),
	}
	go group.run()
	return group
}

func (g *_group) routeToSender(msg *_message) {
	connector := msg.getArg("connector").(*_connector)
	if s, ok := g.senders[connector]; !ok {
		msg.swap()
		msg.Source = group
		msg.Status = sender_not_exists_error
		g.route(msg)
	} else {
		go func() {
			s.workflow <- msg
		}()
	}
}

func (g *_group) routeToQueue(msg *_message) {
	go func() {
		g.queue.controlflow <- msg
	}()
}

func (g *_group) route(msg *_message) {
	msg.addArg("group", g.name)
	switch msg.Destination {
	case sender:
		g.routeToSender(msg)
	default:
		g.routeToQueue(msg)
	}
}

func (g *_group) broadcast(msg *_message) {
	if msg.Destination == sender {
		msg.addArg("group", g.name)
		for _, s := range g.senders {
			go func() {
				s.workflow <- msg.copy()
			}()
		}
	}
}

func (g *_group) removeGroup(msg *_message) {
	ctlMsg := newMessage(ctl, group, sender, stop_sender, undefined_status)
	g.broadcast(ctlMsg)
}

func (g *_group) removeSender(msg *_message) {
	connector := msg.getArg("connector").(*_connector)
	delete(g.senders, connector)
	index := -1
	for i, s := range g.putList {
		if s.connector == connector {
			index = i
			break
		}
	}
	if index != -1 {
		g.numSender -= 1
		newList := make([]*_sender, g.numSender)
		copy(newList[:index], g.putList[:index])
		copy(newList[index:], g.putList[index+1:])
		g.putList = newList
	}
}

func (g *_group) addSender(msg *_message) {
	if msg.Source != client {
		msg.swap()
		msg.Type = rep
		msg.Status = role_error
		g.route(msg)
		return
	}
	connector := msg.getArg("connector").(*_connector)
	if _, ok := g.senders[connector]; ok {
		msg.swap()
		msg.Type = rep
		msg.Status = sender_exists_error
		g.route(msg)
	} else {
		s := newSender(connector, g.controlflow)
		g.senders[connector] = s
		g.putList = append(g.putList, s)
		g.numSender += 1
		msg.swap()
		msg.Type = rep
		msg.Status = success
		g.route(msg)
	}
}

func (g *_group) startSender(msg *_message) {
	go g.senders[msg.getArg("connector").(*_connector)].run()
	msg.Type = rep
	msg.Destination = sender
	msg.Status = success
	g.route(msg)
}

func (g *_group) put(msg *_message) {
	if g.numSender == 0 {
		return
	}
	msg.addArg("group", g.name)
	msg.Destination = sender
	i := int64(g.index) % g.numSender
	go func() {
		g.putList[i].workflow <- msg
	}()
	g.index += 1
}

var groupHandlerMap = map[msgType]map[method]func(*_group, *_message){
	ctl: map[method]func(*_group, *_message){
		add_sender:    (*_group).addSender,
		start_sender:  (*_group).startSender,
		remove_sender: (*_group).removeSender,
	},
	rep: map[method]func(*_group, *_message){},
	act: map[method]func(*_group, *_message){
		put: (*_group).put,
	},
}

func (g *_group) handle(msg *_message) {
	h := groupHandlerMap[msg.Type][msg.Method]
	if h == nil {
		return
	}
	h(g, msg)
}

func (g *_group) run() {
	for {
		select {
		case <-g.stopChan:
			return
		case <-g.pauseChan:
			<-g.pauseChan
		case msg := <-g.workflow:
			if msg.Destination == group {
				g.handle(msg)
			} else {
				g.route(msg)
			}
		case msg := <-g.controlflow:
			if msg.Destination == group {
				g.handle(msg)
			} else {
				g.route(msg)
			}
		}
	}
}
