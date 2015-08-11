package model

import "fmt"

/*
A WrappedError is an error with a short descriptive text added, such as a
function name. This is essentially equivalent to fmt.Errorf("foo: %v",
someError), however it preserves the original error for future inspection and
provides a few convenience methods to make routinely wrapping errors less
painful. The following illustrates two methods for adding context information
to returned errors:

	func connect1() (net.Conn, error) {
		if _, err := net.Dial("tcp", "192.168.0.1:8080"); err != nil {
			return nil, wrapError(err, "connecting")
		}
		return conn, nil
	}

	func connect2() (c net.Conn, wrappedError error) {
		defer wrapErrorPointer(&wrappedError, "connecting")

		if conn, err := net.Dial("tcp", "192.168.0.1:8080"); err != nil {
			return nil, err
		}
		return conn, nil
	}

The second version (named return value, defer wrapErrorPointer) is useful when
there are many returns from a given function and adding wrapping to each would
be cumbersome.
*/
type WrappedError struct {
	Wrapped     error
	Description string
}

func (e WrappedError) Error() string {
	return fmt.Sprintf("%s: %v", e.Description, e.Wrapped)
}

// Original returns the original error prior to wrapping, unpacking all levels
// of wrapping that may have happened since.
func (e WrappedError) Original() error {
	err := e.Wrapped
	inner, ok := e.Wrapped.(WrappedError)
	for ok {
		err = inner.Wrapped
		inner, ok = inner.Wrapped.(WrappedError)
	}
	return err
}

// wrapError returns a WrappedError of the given error, with the description
// string added. If the given error is nil, a nil error is returned.
func wrapError(err error, description string) error {
	if err == nil {
		return nil
	}
	return WrappedError{
		Wrapped:     err,
		Description: description,
	}
}

// wrapErrorPointer replaces the pointed to error with a WrappedError using
// wrapError(). This is primarily useful in defer calls on named return
// values.
func wrapErrorPointer(errp *error, description string) {
	*errp = wrapError(*errp, description)
}
