package semantic

import (
	"fmt"
	"strconv"
	"strings"
)

// Version struct holds semantic versioning information
type Version struct {
	Major int
	Minor int
	Patch int
}

// VersionFromString creates new semantic Version by parsing version string
func VersionFromString(version string) (Version, error) {
	parts := strings.Split(version, ".")

	var ret Version

	if len(parts) > 0 {
		major, err := strconv.Atoi(parts[0])

		if err != nil {
			return ret, fmt.Errorf("major is not a number: %w", err)
		}

		ret.Major = major
	}

	if len(parts) > 1 {
		minor, err := strconv.Atoi(parts[1])

		if err != nil {
			return ret, fmt.Errorf("minor is not a number: %w", err)
		}

		ret.Minor = minor
	}

	if len(parts) > 2 {
		patch, err := strconv.Atoi(parts[2])

		if err != nil {
			return ret, fmt.Errorf("patch is not a number: %w", err)
		}

		ret.Patch = patch
	}

	return ret, nil
}

// Lt returns true if this version is less than version parameter
func (v Version) Lt(version Version) bool {
	if v.Major < version.Major {
		return true
	}

	if v.Minor < version.Minor {
		return true
	}

	return v.Patch < version.Patch
}
