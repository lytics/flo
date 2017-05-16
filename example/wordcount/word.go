package main

func (c *Word) Combine(other *Word) {
	if c.Text != other.Text {
		panic("text not equal")
	}
	c.Count += other.Count
}
