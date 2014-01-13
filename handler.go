package estuary

import "log"

var Handlers = make([]Handler, 0)

type Handler struct {
	Name string
	BindingKey string
	Exclusive bool
	HandlerFunc func(Delivery)
}

func RegisterHandler(name string, bindingKey string, exclusive bool, f func(Delivery)) {
	for _, handler := range Handlers {
		if handler.Name == name {
			log.Fatalf("Duplicate handler name '%s'. Handlers must have unique names", name)
		}
	}
	Handlers = append(Handlers, Handler{name, bindingKey, exclusive, f})
}