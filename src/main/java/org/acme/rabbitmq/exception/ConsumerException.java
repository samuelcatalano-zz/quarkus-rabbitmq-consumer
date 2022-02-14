package org.acme.rabbitmq.exception;

public class ConsumerException extends Exception {

    public ConsumerException() {
        super();
    }

    public ConsumerException(String message) {
        super(message);
    }

    public ConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConsumerException(Throwable cause) {
        super(cause);
    }
}
