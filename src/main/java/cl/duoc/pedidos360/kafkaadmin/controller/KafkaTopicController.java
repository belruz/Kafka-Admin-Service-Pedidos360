package cl.duoc.pedidos360.kafkaadmin.controller;

import cl.duoc.pedidos360.kafkaadmin.dto.TopicConfigUpdateRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicCreateRequest;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaTopicService;

import jakarta.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/admin/kafka/topics")
public class KafkaTopicController {

    private final KafkaTopicService topicService;

    public KafkaTopicController(KafkaTopicService topicService) {
        this.topicService = topicService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> crear(@Valid @RequestBody TopicCreateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(topicService.crearTopico(request));
    }

    @GetMapping
    public List<String> listar() {
        return topicService.listarTopicos();
    }

    @GetMapping("/{name}")
    public Map<String, Object> detalle(@PathVariable String name) {
        return topicService.describirTopico(name);
    }

    @PatchMapping("/{name}")
    public Map<String, Object> actualizar(
            @PathVariable String name,
            @Valid @RequestBody TopicConfigUpdateRequest request) {
        return topicService.actualizarTopico(name, request);
    }

    @DeleteMapping("/{name}")
    public Map<String, Object> eliminar(@PathVariable String name) {
        return topicService.eliminarTopico(name);
    }
}
