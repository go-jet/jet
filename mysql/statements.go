package mysql

import "github.com/go-jet/jet/internal/jet"

// ----------------- FUNCTIONS ----------------------//

var SELECT = jet.SELECT

type SelectLock jet.SelectLock

var (
	UPDATE = jet.NewSelectLock("UPDATE")
	SHARE  = jet.NewSelectLock("SHARE")
)

var UNION = jet.UNION
var UNION_ALL = jet.UNION_ALL

//-----------------literals----------------------//

var STAR = jet.STAR
var NULL = jet.NULL
var DEFAULT = jet.DEFAULT
