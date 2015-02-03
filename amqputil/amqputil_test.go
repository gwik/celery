package amqputil

import (
	"net"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

func TestAMQPRetry_exitOnConfigError(t *testing.T) {
	retrier := NewRetry("thisiswrong", nil, 2*time.Second)
	chch := retrier.Channel()
	defer func() {
		err := retrier.Close()
		if err == nil {
			t.Fatal("Err should be set.")
		}
	}()

	_, ok := <-chch
	if ok {
		t.Fatal("Should not be ok connection should have failed.")
	}

}

type fakeErr struct {
}

func (fakeErr) Temporary() bool {
	return false
}

func (fakeErr) Error() string {
	return "fake error."
}

func (fakeErr) Timeout() bool {
	return false
}

func TestAMQPRetry_continueOnNetError(t *testing.T) {
	config := &amqp.Config{
		Dial: func(string, string) (net.Conn, error) {
			return nil, fakeErr{}
		},
	}
	retrier := NewRetry("amqp://127.0.13.2:64987//", config, time.Duration(2*time.Second))
	defer retrier.Close()
	<-time.After(1 * time.Millisecond)
	chch := retrier.Channel()
	select {
	case <-chch:
		t.Fatal("Should wait connection.")
	default:
	}
}
