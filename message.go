package nbmq

type msgType string

const (
	ctl msgType = "ctl"
	rep         = "rep"
	act         = "act"
)

type role string

const (
	undefined_role role = "undefined_role"
	listener            = "listener"
	receiver            = "receiver"
	queue               = "queue"
	group               = "group"
	sender              = "sender"
	client              = "client"
	reader              = "reader"
)

type method string

const (
	undefined_method       method = "undefined_method"
	remove_receiver               = "remove_receiver"
	remove_sender                 = "remove_sender"
	remove_group                  = "remove_group"
	remove_queue                  = "remove_queue"
	remove_client                 = "remove_client"
	start_receiver                = "start receiver"
	stop_receiver                 = "stop_receiver"
	start_sender                  = "start sender"
	stop_sender                   = "stop_sender"
	stop_group                    = "stop_group"
	stop_queue                    = "stop_queue"
	stop_client                   = "stop_client"
	stop_and_remove_client        = "stop_and_remove_client"
	add_queue                     = "add_queue"
	add_receiver                  = "add_receiver"
	add_sender                    = "add_sender"
	add_group                     = "add_group"
	close_client                  = "close_client"
	close_receiver                = "close_receiver"
	close_sender                  = "close_sender"
	queues_info                   = "queues_info"
	put                           = "put"
)

type status string

const (
	undefined_status            status = "undefined_status"
	success                            = "success"
	conn_read_error                    = "conn_read_error"
	conn_write_error                   = "conn_write_error"
	unmarshal_message_error            = "unmarshal_message_error"
	marshal_message_error              = "marshal_message_error"
	topic_arg_error                    = "topic argument not in ArgMap"
	no_topic_arg_error                 = "no_topic_arg_error"
	no_topic_error                     = "no_topic_error"
	no_group_error                     = "no_group_error"
	topic_exists_error                 = "topic_exists_error"
	receiver_exists_error              = "receiver_exists_error"
	receiver_not_exists_error          = "receiver_not_exists_error"
	sender_exists_error                = "sender_exists_error"
	sender_not_exists_error            = "sender_not_exists_error"
	group_exists_error                 = "group_exists_error"
	group_not_exists_error             = "group_not_exists_error"
	client_not_exists_error            = "client_not_exists_error"
	queue_not_exists_error             = "queue_not_exists_error"
	unknown_role_error                 = "unknown_role_error"
	not_valid_destination_error        = "not_valid_destination_error"
	connector_write_error              = "connector message channel has closed"
	role_error                         = "role error"
	unknown_method                     = "unknown_method"
)

type _message struct {
	Type        msgType
	Source      role
	Destination role
	Method      method
	ArgMap      map[string]interface{}
	Data        []byte
	Status      status
}

func (m *_message) copy() *_message {
	return &_message{
		Type:        m.Type,
		Source:      m.Source,
		Destination: m.Destination,
		Method:      m.Method,
		ArgMap:      m.ArgMap,
		Data:        m.Data,
		Status:      m.Status,
	}
}

func newMessage(typ msgType, source, destination role, method method, status status) *_message {
	return &_message{
		Type:        typ,
		Source:      source,
		Destination: destination,
		Method:      method,
		ArgMap:      make(map[string]interface{}),
		Status:      status,
	}
}

func (m *_message) addArg(key string, value interface{}) {
	m.ArgMap[key] = value
}

func (m *_message) swap() {
	m.Source, m.Destination = m.Destination, m.Source
}

func (m *_message) getArg(key string) interface{} {
	return m.ArgMap[key]
}
