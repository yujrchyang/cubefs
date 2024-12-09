package collection

// Contains if the element of longArr contains all elements in shortArr, return true
func Contains(longArr, shortArr []string) bool {
	m := make(map[string]int)
	for _, v := range longArr {
		m[v] = 1
	}
	for _, v := range shortArr {
		_, ok := m[v]
		if !ok {
			return false
		}
	}
	return true
}
