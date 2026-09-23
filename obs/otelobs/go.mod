module github.com/abdussalamfaqih/retryx/obs/otelobs

go 1.25.0

replace github.com/abdussalamfaqih/retryx => ../..

require (
	github.com/abdussalamfaqih/retryx v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/metric v1.46.0
	go.opentelemetry.io/otel/trace v1.46.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
)
