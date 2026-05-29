package jet

import "github.com/go-jet/jet/v2/internal/3rdparty/snaker"

// Config holds the configuration settings for Jet
type Config struct {
	Snaker *snaker.Config
}

// GlobalConfig is the package-wide configuration
// This variable is not thread safe, and it should be modified only once, for instance, during application initialization.
var GlobalConfig = Config{
	Snaker: &snaker.GlobalConfig,
}
