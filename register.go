package xk6_kafka_rest

import "go.k6.io/k6/js/modules"

func init() {
	modules.Register("k6/x/kafka-rest", new(RootModule))
}

type RootModule struct{}

type KafkaRestModule struct {
	vu      modules.VU
	metrics *kafkaMetrics
}

var (
	_ modules.Module   = &RootModule{}
	_ modules.Instance = &KafkaRestModule{}
)

func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &KafkaRestModule{
		vu:      vu,
		metrics: registerMetrics(vu.InitEnv().Registry),
	}
}

func (m *KafkaRestModule) Exports() modules.Exports {
	return modules.Exports{
		Named: map[string]interface{}{
			"KafkaRestClient": m.newKafkaRestClient,
		},
	}
}
