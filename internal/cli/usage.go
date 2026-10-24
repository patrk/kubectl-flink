package cli

import "strings"

// cobra returns plain errors for flag and argument problems; these prefixes identify them.
var usagePrefixes = []string{"unknown command", "unknown flag", "unknown shorthand flag", "invalid argument",
	"flag needs an argument", "accepts ", "requires ", "required flag"}

func isUsageError(err error) bool {
	msg := err.Error()
	for _, p := range usagePrefixes {
		if strings.HasPrefix(msg, p) {
			return true
		}
	}
	return false
}
