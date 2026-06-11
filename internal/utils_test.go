package internal

import (
	"testing"
)

func TestBytesToString(t *testing.T) {
	tests := []struct {
		input []byte
		want  string
	}{
		{[]byte("hello"), "hello"},
		{[]byte(""), ""},
		{nil, ""},
		{[]byte("a"), "a"},
		{[]byte("hello world"), "hello world"},
		{[]byte{0xFF, 0xFE, 0xFD}, "\xff\xfe\xfd"},
	}
	for _, tt := range tests {
		got := BytesToString(tt.input)
		if got != tt.want {
			t.Fatalf("BytesToString(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestStringToBytes(t *testing.T) {
	tests := []struct {
		input string
		want  []byte
	}{
		{"hello", []byte("hello")},
		{"", nil},
		{"a", []byte("a")},
		{"hello world", []byte("hello world")},
		{"\xff\xfe\xfd", []byte{0xFF, 0xFE, 0xFD}},
	}
	for _, tt := range tests {
		got := StringToBytes(tt.input)
		if len(got) == 0 && len(tt.want) == 0 {
			continue
		}
		if string(got) != string(tt.want) {
			t.Fatalf("StringToBytes(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestStringBytesRoundtrip(t *testing.T) {
	original := "hello, 世界! \xff\xfe"
	b := StringToBytes(original)
	s := BytesToString(b)
	if s != original {
		t.Fatalf("roundtrip failed: %q != %q", s, original)
	}
}

func TestBytesStringRoundtrip(t *testing.T) {
	original := []byte("hello, 世界! \xff\xfe")
	s := BytesToString(original)
	b := StringToBytes(s)
	if string(b) != string(original) {
		t.Fatalf("roundtrip failed: %q != %q", string(b), string(original))
	}
}

func TestStringToBytesLarge(t *testing.T) {
	s := string(make([]byte, 10000))
	b := StringToBytes(s)
	if len(b) != len(s) {
		t.Fatalf("length mismatch: %d vs %d", len(b), len(s))
	}
}

func TestBytesToStringLarge(t *testing.T) {
	b := make([]byte, 10000)
	s := BytesToString(b)
	if len(s) != len(b) {
		t.Fatalf("length mismatch: %d vs %d", len(s), len(b))
	}
}

func TestBytesToStringNilSafety(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("BytesToString(nil) panicked")
		}
	}()
	_ = BytesToString(nil)
}

func TestStringToBytesEmpty(t *testing.T) {
	b := StringToBytes("")
	if b != nil {
		t.Fatal("StringToBytes(\"\") should return nil")
	}
}
