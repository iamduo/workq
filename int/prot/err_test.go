package prot

import "testing"

func TestNewClientErr(t *testing.T) {
	err := NewClientErr("ERR")

	if err.Error() != "CLIENT-ERROR ERR\r\n" {
		t.Fatalf("ClientErr string mismatch, act=%+v", err.Error())
	}

	err = NewClientErr("")
	if err.Error() != "CLIENT-ERROR\r\n" {
		t.Fatalf("ClientErr string mismatch, act=%+v", err.Error())
	}
}

func TestNewServerErr(t *testing.T) {
	err := NewServerErr("ERR")

	if err.Error() != "SERVER-ERROR ERR\r\n" {
		t.Fatalf("ServerErr string mismatch, act=%v", err.Error())
	}

	err = NewServerErr("")
	if err.Error() != "SERVER-ERROR\r\n" {
		t.Fatalf("ServerErr string mismatch, act=%v", err.Error())
	}
}
