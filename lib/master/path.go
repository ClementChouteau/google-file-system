package master

import (
	"errors"
	"strings"
)

func normalize(path string) (string, error) {
	if len(path) == 0 {
		return "", errors.New("empty path")
	}
	if !strings.HasPrefix(path, "/") {
		return "", errors.New("not an absolute path")
	}

	var builder strings.Builder
	builder.Grow(len(path))
	// Remove duplicated consecutive '/'
	prev := rune(0)
	for _, l := range path {
		if l != '/' || prev != '/' {
			builder.WriteRune(l)
		}
		prev = l
	}

	// Remove trailing '/'
	normalized, _ := strings.CutSuffix(builder.String(), "/")

	if len(normalized) == 0 {
		normalized += "/"
	}

	return normalized, nil
}

// Return the parent directory, example: "/a/b" => "/a'
func parentPath(path string) string {
	i := strings.LastIndex(path, "/")
	if i <= 0 {
		return "/"
	}
	return path[:i]
}
