package main

func Example() {
	main()

	// Output:
	// bus: one request for east, north, south
	// north: 21.5°C
	// south: 23.0°C
	// east: 19.0°C
	// bus: one request for east, north, south
	// log: subscriber 1: bus down
	// north: subscriber 1: bus down
	// south: subscriber 1: bus down
	// east: subscriber 1: bus down
}
