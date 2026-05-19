// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Small .env loader. Sets keys via os.Setenv for any k=v lines found,
// without interpreting `$var` substitutions (so passwords containing $
// aren't mangled by shell `source`). Lines starting with '#' are ignored.
// Existing OS env takes precedence (never overwrites).
package db

import (
	"bufio"
	"log"
	"os"
	"strings"
)

// LoadDotEnv reads one or more paths, first-existing-wins. Missing files
// are silently skipped; malformed lines are skipped. Safe to call at process
// startup before reading env via os.Getenv.
func LoadDotEnv(paths ...string) {
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		loaded := 0
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			_ = loaded
			raw := strings.TrimSpace(sc.Text())
			if raw == "" || strings.HasPrefix(raw, "#") {
				continue
			}
			eq := strings.IndexByte(raw, '=')
			if eq <= 0 {
				continue
			}
			k := strings.TrimSpace(raw[:eq])
			v := strings.TrimSpace(raw[eq+1:])
			// Strip surrounding single or double quotes, if present.
			if len(v) >= 2 {
				if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
					v = v[1 : len(v)-1]
				}
			}
			if _, exists := os.LookupEnv(k); exists {
				continue
			}
			_ = os.Setenv(k, v)
			loaded++
		}
		f.Close()
		cwd, _ := os.Getwd()
		log.Printf("[env] loaded %d keys from %s (cwd=%s)", loaded, p, cwd)
		return
	}
	cwd, _ := os.Getwd()
	log.Printf("[env] no .env found among %v (cwd=%s)", paths, cwd)
}
