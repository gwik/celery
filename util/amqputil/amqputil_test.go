package amqputil

import (
	"testing"
	"time"
)

func TestAMQPRetry_exitOnConfigError(t *testing.T) {

	retrier := NewAMQPRetry("thisiswrong", nil, 2*time.Second)

	chch := retrier.Channel()

	_, ok := <-chch
	if ok {
		t.Fatal("Should not be ok connection should have failed.")
	}

	err := retrier.Err()
	if err == nil {
		t.Fatal("Err should be set.")
	}

	err = retrier.Close()
	if err == nil {
		t.Fatal("Err should be set.")
	}

}

func TestAMQPRetry_continueOnTemporaryError(t *testing.T) {

	retrier := NewAMQPRetry("amqp://127.0.13.2:64987//", nil, time.Duration(0))
	defer func() {
		err := retrier.Close()
		if err != nil {
			t.Errorf("err should be nil was %v", err)
		}
	}()

	chch := retrier.Channel()

	select {
	case <-chch:
		t.Fatal("Should wait connection.")
	default:
	}

	if retrier.Err() != nil {
		t.Fatal("No error should be set")
	}
}
