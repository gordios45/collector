// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package noaa_tsunami

import (
	"encoding/xml"
	"testing"
)

func TestAtomUnmarshalNamespacedGeo(t *testing.T) {
	raw := []byte(`<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom" xmlns:geo="http://www.w3.org/2003/01/geo/wgs84_pos#"><title>Feed</title><entry><id>urn:test</id><title>Region</title><updated>2026-04-25T08:08:42Z</updated><geo:lat>53.656</geo:lat><geo:long>-164.975</geo:long></entry></feed>`)
	var feed atomFeed
	if err := xml.Unmarshal(raw, &feed); err != nil {
		t.Fatal(err)
	}
	if len(feed.Entries) != 1 {
		t.Fatalf("entries=%d", len(feed.Entries))
	}
	if feed.Entries[0].Lat != "53.656" || feed.Entries[0].Lon != "-164.975" {
		t.Fatalf("lat/lon not unmarshaled: %+v", feed.Entries[0])
	}
}

func TestEventFromEntry(t *testing.T) {
	feed := atomFeed{Title: "Tsunami Information Statement Number 1", Author: atomAuthor{Name: "NWS National Tsunami Warning Center"}}
	entry := atomEntry{
		ID:      "urn:uuid:test",
		Title:   "65 miles SE of Dutch Harbor, Alaska",
		Updated: "2026-04-25T08:08:42Z",
		Lat:     "53.656",
		Lon:     "-164.975",
		Summary: innerXML{Inner: `<div><strong>Category:</strong> Information<br/><strong>Preliminary Magnitude: </strong>5.1(Ml)<br/><strong>Affected Region: </strong>65 miles SE of Dutch Harbor, Alaska<br/><b>Note:</b>  * There is NO tsunami danger from this earthquake.</div>`},
		Links: []atomLink{
			{Title: "CapXML document", Href: "https://example.test/cap.xml", Type: "application/cap+xml"},
			{Title: "Bulletin", Href: "https://example.test/bulletin.txt", Type: "application/xml"},
		},
	}
	ev, ok := eventFromEntry("NTWC", ntwcURL, feed, entry)
	if !ok {
		t.Fatal("event skipped")
	}
	if ev.Source != "noaa_tsunami" || ev.ExtID != "urn:uuid:test" {
		t.Fatalf("event identity wrong: %+v", ev)
	}
	if ev.Lat != 53.656 || ev.Lon != -164.975 {
		t.Fatalf("lat/lon wrong: %.3f %.3f", ev.Lat, ev.Lon)
	}
	if ev.Props["category"] != "Information" {
		t.Fatalf("category=%v", ev.Props["category"])
	}
	if ev.Props["magnitude"] != "5.1(Ml)" {
		t.Fatalf("magnitude=%v", ev.Props["magnitude"])
	}
	if ev.Props["cap_url"] != "https://example.test/cap.xml" {
		t.Fatalf("cap_url=%v", ev.Props["cap_url"])
	}
}
