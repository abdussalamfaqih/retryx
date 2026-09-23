module example.com/retryx/examples/prometheus

go 1.21

require (
	example.com/retryx v0.0.0
	example.com/retryx/obs/promobs v0.0.0
)

// Local development: use the packages from this repository.
// (replace directives of dependencies are ignored, so both are listed here)
replace example.com/retryx => ../..

replace example.com/retryx/obs/promobs => ../../obs/promobs

// Run `go mod tidy` once: it adds github.com/prometheus/client_golang and go.sum.
