module github.com/abdussalamfaqih/retryx/examples/prometheus

go 1.21

require (
	github.com/abdussalamfaqih/retryx v0.0.0
	github.com/abdussalamfaqih/retryx/obs/promobs v0.0.0
)

// Local development: use the packages from this repository.
// (replace directives of dependencies are ignored, so both are listed here)
replace github.com/abdussalamfaqih/retryx => ../..

replace github.com/abdussalamfaqih/retryx/obs/promobs => ../../obs/promobs

// Run `go mod tidy` once: it adds github.com/prometheus/client_golang and go.sum.
