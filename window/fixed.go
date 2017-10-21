package window

import "time"

// Fixed window of width time, same as a sliding
// window where width and period are euqal.
func Fixed(width time.Duration) Window {
	return Sliding(width, width)
}
