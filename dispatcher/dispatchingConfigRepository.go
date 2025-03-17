package dispatcher

type IDispatchingConfigRepository interface {
	Get(eventType string) (cfg DispatchingConfig, ok bool)
}
