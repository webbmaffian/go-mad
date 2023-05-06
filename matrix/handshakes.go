package matrix

import "math"

func handshakes(n int) int {
	return n * (n - 1) / 2
}

func countFromHandshakes(n int) int {
	return int(math.Ceil(math.Sqrt(float64(n) * 2)))
}
