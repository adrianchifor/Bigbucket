package utils

// Returns index if found, otherwise -1
func Search(list []string, a string) int {
	for i, elem := range list {
		if elem == a {
			return i
		}
	}
	return -1
}

func RemoveIndex(list []string, index int) []string {
	list[index] = list[len(list)-1]
	list[len(list)-1] = ""
	list = list[:len(list)-1]

	return list
}

func MergeMaps(maps ...map[string]string) map[string]string {
	mergedMap := make(map[string]string)
	for _, innerMap := range maps {
		for k, v := range innerMap {
			mergedMap[k] = v
		}
	}

	return mergedMap
}
