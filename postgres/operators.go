package postgres

import "github.com/go-jet/jet/v2/internal/jet"

// NOT returns negation of bool expression result
var NOT = jet.NOT

// BIT_NOT inverts every bit in integer expression result
var BIT_NOT = jet.BIT_NOT

// DISTINCT operator can be used to return distinct values of expr
var DISTINCT = jet.DISTINCT
