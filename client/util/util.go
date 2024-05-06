package util

import "strings"

const (
	volSeparator = "-"
)

// Strip sharding, role and index infomation for volume names from mysql cluster, this will make monitoring a given database much simpler.
func GetNormalizedVolName(volName string) string {
	parts := strings.Split(volName, volSeparator)
	if len(parts) < 3 {
		return volName
	}

	// coraldb volume names don't have role and index information
	if parts[len(parts)-2] != "rdonly" && parts[len(parts)-2] != "replica" {
		if parts[len(parts)-1] == "0" {
			// db has only one sharding, e.g. db-lf86a-0
			return strings.Join(parts[:len(parts)-2], volSeparator)
		} else if len(parts) >= 4 {
			// db has multiple shardings, e.g. db-lf86a-xx-80
			return strings.Join(parts[:len(parts)-3], volSeparator)
		}
	}

	if len(parts) >= 5 && parts[len(parts)-3] == "0" {
		// db has only one sharding, e.g. db-ht3e-0-rdonly-0
		volName = strings.Join(parts[:len(parts)-4], volSeparator)
	} else if len(parts) >= 6 && parts[len(parts)-3] != "0" {
		// db has multiple shardings, e.g. db-ht3e-xx-80-rdonly-0
		volName = strings.Join(parts[:len(parts)-5], volSeparator)
	}
	return volName
}
