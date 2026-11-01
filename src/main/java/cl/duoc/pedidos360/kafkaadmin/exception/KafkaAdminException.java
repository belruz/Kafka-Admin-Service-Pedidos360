package cl.duoc.pedidos360.kafkaadmin.exception;

import org.springframework.http.HttpStatus;

public class KafkaAdminException extends RuntimeException {

    private final HttpStatus status;

    public KafkaAdminException(HttpStatus status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public KafkaAdminException(HttpStatus status, String message) {
        this(status, message, null);
    }

    public HttpStatus getStatus() {
        return status;
    }
}
