// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/gordios45/collector/internal/importers/aircraftregistry"
	"github.com/gordios45/collector/internal/importers/carrieradvisories"
	"github.com/gordios45/collector/internal/importers/countryboundaries"
	"github.com/gordios45/collector/internal/importers/featureseeds"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	subcmd, subargs := args[0], args[1:]
	switch subcmd {
	case "features", "seed":
		featureseeds.Main(subargs)
	case "country-boundaries", "countries":
		countryboundaries.Main(subargs)
	case "aircraft-registry", "aircraft":
		aircraftregistry.Main(subargs)
	case "carrier-advisories", "carriers":
		carrieradvisories.Main(subargs)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown importer subcommand %q\n\n", subcmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `Usage:
  go run ./cmd/importer <subcommand> [flags] [args]

Subcommands:
  features             seed generic static/background features
  country-boundaries   import Natural Earth country boundaries
  aircraft-registry    import OpenSky aircraft registry CSV
  carrier-advisories   import airline advisory workbooks

Examples:
  go run ./cmd/importer features -list
  go run ./cmd/importer features -cache-dir=.cache/features chokepoints cables
  go run ./cmd/importer features -refresh cables
  go run ./cmd/importer country-boundaries
  go run ./cmd/importer aircraft-registry
  go run ./cmd/importer carrier-advisories --dir=../tmp_resources
`)
}
