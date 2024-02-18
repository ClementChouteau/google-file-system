package arraySet

func Contains[T comparable](array []T, haystack T) bool {
	for _, value := range array {
		if value == haystack {
			return true
		}
	}
	return false
}

func Insert[T comparable](array []T, value T) []T {
	if Contains(array, value) {
		return array
	}

	return append(array, value)
}

func Remove[T comparable](array []T, value T) []T {
	if !Contains(array, value) {
		return array
	}

	var result []T
	for _, v := range array {
		if v != value {
			result = append(result, v)
		}
	}

	return result
}

// Diff (arr1, arr2) (added, removed)
func Diff[T comparable](arr1 []T, arr2 []T) ([]T, []T) {
	set1 := make(map[T]bool)
	for _, value := range arr1 {
		set1[value] = true
	}

	set2 := make(map[T]bool)
	for _, value := range arr2 {
		set2[value] = true
	}

	// Find added elements by checking elements in arr2 that are not in arr1
	added := make([]T, 0, len(arr2))
	for _, value := range arr2 {
		if _, exists := set1[value]; !exists {
			added = append(added, value)
		}
	}

	// Find removed elements by checking elements in arr1 that are not in arr2
	removed := make([]T, 0, len(arr1))
	for _, value := range arr1 {
		if _, exists := set2[value]; !exists {
			removed = append(removed, value)
		}
	}

	return added, removed
}
