package cl.duoc.pedidos360.kafkaadmin.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(KafkaAdminException.class)
    public ResponseEntity<Map<String, Object>> handleKafka(KafkaAdminException ex) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("estado", "ERROR");
        body.put("mensaje", ex.getMessage());
        if (ex.getCause() != null) {
            body.put("detalle", ex.getCause().getMessage());
        }
        return ResponseEntity.status(ex.getStatus()).body(body);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> errores = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(e -> errores.put(e.getField(), e.getDefaultMessage()));

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("estado", "ERROR");
        body.put("mensaje", "Datos de entrada inválidos");
        body.put("errores", errores);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }
}
