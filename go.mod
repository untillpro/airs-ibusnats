module github.com/untillpro/airs-ibusnats

go 1.12

require (
	github.com/golang/protobuf v1.3.2 // indirect
	github.com/nats-io/gnatsd v1.4.1 // indirect
	github.com/nats-io/go-nats v1.7.0
	github.com/nats-io/nkeys v0.1.3 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/stretchr/testify v1.4.0 // indirect
	github.com/untillpro/airs-ibus v0.0.0-20200121070139-14a815e68007
	github.com/untillpro/gochips v1.9.0
	github.com/untillpro/godif v0.12.0
)

replace github.com/untillpro/airs-ibus => ../airs-ibus
