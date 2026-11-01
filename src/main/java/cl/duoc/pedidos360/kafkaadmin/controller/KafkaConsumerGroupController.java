package cl.duoc.pedidos360.kafkaadmin.controller;

import cl.duoc.pedidos360.kafkaadmin.dto.OffsetResetRequest;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaConsumerGroupService;

import jakarta.validation.Valid;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/admin/kafka/consumer-groups")
public class KafkaConsumerGroupController {

    private final KafkaConsumerGroupService groupService;

    public KafkaConsumerGroupController(KafkaConsumerGroupService groupService) {
        this.groupService = groupService;
    }

    @GetMapping
    public List<Map<String, Object>> listar() {
        return groupService.listarGrupos();
    }

    @GetMapping("/{groupId}")
    public Map<String, Object> detalle(@PathVariable String groupId) {
        return groupService.describirGrupo(groupId);
    }

    @GetMapping("/{groupId}/offsets")
    public List<Map<String, Object>> offsets(@PathVariable String groupId) {
        return groupService.obtenerOffsets(groupId);
    }

    @PostMapping("/{groupId}/reset-offsets")
    public Map<String, Object> reiniciarOffsets(
            @PathVariable String groupId,
            @Valid @RequestBody OffsetResetRequest request) {
        return groupService.reiniciarOffsets(groupId, request.getTopico(), request.getPosicion());
    }

    @DeleteMapping("/{groupId}")
    public Map<String, Object> eliminar(@PathVariable String groupId) {
        return groupService.eliminarGrupo(groupId);
    }
}
