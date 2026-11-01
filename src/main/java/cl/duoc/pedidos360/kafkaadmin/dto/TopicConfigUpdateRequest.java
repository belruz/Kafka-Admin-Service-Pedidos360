package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

/**
 * DTO para modificar la configuración de un tópico existente.
 * Solo se aplican los campos informados.
 */
public class TopicConfigUpdateRequest {

    /** Nuevo total de particiones (solo se puede aumentar). */
    @Min(value = 1, message = "Las particiones deben ser al menos 1")
    private Integer particiones;

    private Long retencionMs;

    private Long retencionBytes;

    @Pattern(regexp = "^(delete|compact|compact,delete)?$",
            message = "cleanupPolicy debe ser delete, compact o compact,delete")
    private String cleanupPolicy;

    public Integer getParticiones() { return particiones; }
    public void setParticiones(Integer particiones) { this.particiones = particiones; }

    public Long getRetencionMs() { return retencionMs; }
    public void setRetencionMs(Long retencionMs) { this.retencionMs = retencionMs; }

    public Long getRetencionBytes() { return retencionBytes; }
    public void setRetencionBytes(Long retencionBytes) { this.retencionBytes = retencionBytes; }

    public String getCleanupPolicy() { return cleanupPolicy; }
    public void setCleanupPolicy(String cleanupPolicy) { this.cleanupPolicy = cleanupPolicy; }
}
