package version

import "testing"

func TestHelloWorld(t *testing.T) {
	if HelloWorld() != "Hello, World!" {
		t.Errorf("Error!")
	}
}
