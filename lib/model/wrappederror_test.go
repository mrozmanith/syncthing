package model

import (
	"errors"
	"testing"
)

func TestWrappedError(t *testing.T) {
	err := errors.New("foo")
	werr1 := wrapError(err, "hey")
	werr2 := wrapError(werr1, "oi")

	if werr1.Error() != "hey: foo" {
		t.Error("incorrect format for werr1:", werr1.Error())
	}
	if werr2.Error() != "oi: hey: foo" {
		t.Error("incorrect format for werr2:", werr2.Error())
	}
}

func TestWrappedErrorNil(t *testing.T) {
	// Wrapping a nil error returns nil, so that "if err != nil" semantics are
	// undisturbed.

	var err error
	werr1 := wrapError(err, "hey")

	if werr1 != nil {
		t.Error("incorrect non-nil werr1")
	}
}

func TestWrappedErrorOriginal(t *testing.T) {
	// Original() returns the first error prior to any wrapping.

	err := errors.New("foo")
	werr1 := wrapError(err, "hey")
	werr2 := wrapError(werr1, "oi")

	if orig := werr1.(WrappedError).Original(); orig != err {
		t.Error("incorrect original for werr1:", orig)
	}
	if orig := werr2.(WrappedError).Original(); orig != err {
		t.Error("incorrect original for werr2:", orig)
	}
}

func TestWrappedErrorDeferred(t *testing.T) {
	err := deferringError()
	if err.Error() != "deferred: fail" {
		t.Error("incorrect format for deferred error:", err.Error())
	}

	if deferringErrorNil() != nil {
		t.Error("incorrect non-nil deferred error")
	}
}

func deferringError() (wrappedError error) {
	defer wrapErrorPointer(&wrappedError, "deferred")
	return errors.New("fail")
}
func deferringErrorNil() (wrappedError error) {
	defer wrapErrorPointer(&wrappedError, "deferred")
	return nil
}
