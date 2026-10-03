package main

import (
	"testing"

	"github.com/go-jet/jet/v2/generator/metadata"
)

func TestMatchesFilter(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		input    string
		want     bool
	}{
		{"exact match", []string{"actor"}, "actor", true},
		{"exact no match", []string{"actor"}, "film", false},
		{"case insensitive input", []string{"actor"}, "ACTOR", true},
		{"underscore is literal", []string{"user_data"}, "user_data", true},
		{"underscore does not match anything", []string{"user_data"}, "userxdata", false},
		{"trailing wildcard", []string{"payment_*"}, "payment_2020", true},
		{"trailing wildcard no match", []string{"payment_*"}, "orders", false},
		{"single char wildcard", []string{"log_?"}, "log_1", true},
		{"single char wildcard too long", []string{"log_?"}, "log_12", false},
		{"one of many patterns", []string{"actor", "film_*"}, "film_category", true},
		{"empty pattern from empty list", []string{""}, "actor", false},
		{"malformed pattern is ignored", []string{"[bad"}, "actor", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesFilter(tt.patterns, tt.input); got != tt.want {
				t.Errorf("matchesFilter(%q, %q) = %v, want %v", tt.patterns, tt.input, got, tt.want)
			}
		})
	}
}

func TestShouldSkipTableIgnore(t *testing.T) {
	filter := templateFilter{names: parseList("payment_*, actor"), ignore: true}

	skipped := []string{"payment_2020", "payment_2021", "actor"}
	for _, name := range skipped {
		if !shouldSkipTable(metadata.Table{Name: name}, filter) {
			t.Errorf("expected table %q to be skipped", name)
		}
	}

	kept := []string{"film", "category"}
	for _, name := range kept {
		if shouldSkipTable(metadata.Table{Name: name}, filter) {
			t.Errorf("expected table %q to be generated", name)
		}
	}
}

func TestShouldSkipTableAllow(t *testing.T) {
	filter := templateFilter{names: parseList("film_*"), ignore: false}

	if shouldSkipTable(metadata.Table{Name: "film_actor"}, filter) {
		t.Error("expected table film_actor to be generated")
	}
	if !shouldSkipTable(metadata.Table{Name: "payment"}, filter) {
		t.Error("expected table payment to be skipped")
	}
}

func TestShouldSkipPartitionTables(t *testing.T) {
	defer func() { skipPartitionTables = false }()

	// an empty ignore filter keeps every table by default
	filter := templateFilter{names: parseList(""), ignore: true}

	partition := metadata.Table{Name: "measurement_y2020", IsPartition: true}
	parent := metadata.Table{Name: "measurement", IsPartition: false}

	skipPartitionTables = false
	if shouldSkipTable(partition, filter) {
		t.Error("partition should be kept when -skip-partition-tables is off")
	}

	skipPartitionTables = true
	if !shouldSkipTable(partition, filter) {
		t.Error("partition should be skipped when -skip-partition-tables is on")
	}
	if shouldSkipTable(parent, filter) {
		t.Error("partitioned parent table should be kept")
	}
}

func TestShouldSkipEnum(t *testing.T) {
	filter := templateFilter{names: parseList("mpaa_*"), ignore: true}

	if !shouldSkipEnum(metadata.Enum{Name: "mpaa_rating"}, filter) {
		t.Error("expected enum mpaa_rating to be skipped")
	}
	if shouldSkipEnum(metadata.Enum{Name: "status"}, filter) {
		t.Error("expected enum status to be generated")
	}
}
