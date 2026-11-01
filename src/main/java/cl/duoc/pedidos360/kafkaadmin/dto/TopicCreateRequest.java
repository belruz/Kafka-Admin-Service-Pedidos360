package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * DTO de creación de tópicos con parámetros avanzados.
 */
public class TopicCreateRequest {

    @NotBlank(message = "El nombre del tópico es obligatorio")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$",
            message = "El nombre solo puede contener letras, números, '.', '_' y '-'")
    private String nombre;

    @Min(value = 1, message = "Las particiones deben ser al menos 1")
    private int particiones = 1;

    @Min(value = 1, message = "El factor de replicación debe ser al menos 1")
    private short factorReplicacion = 1;

    /** Retención en milisegundos (retention.ms). -1 = infinita. Opcional. */
    private Long retencionMs;

    /** Retención en bytes por partición (retention.bytes). -1 = sin límite. Opcional. */
    private Long retencionBytes;

    /** delete | compact | "compact,delete". Opcional (por defecto delete). */
    @Pattern(regexp = "^(delete|compact|compact,delete)?$",
            message = "cleanupPolicy debe ser delete, compact o compact,delete")
    private String cleanupPolicy;

    public String getNombre() { return nombre; }
    public void setNombre(String nombre) { this.nombre = nombre; }

    public int getParticiones() { return particiones; }
    public void setParticiones(int particiones) { this.particiones = particiones; }

    public short getFactorReplicacion() { return factorReplicacion; }
    public void setFactorReplicacion(short factorReplicacion) { this.factorReplicacion = factorReplicacion; }

    public Long getRetencionMs() { return retencionMs; }
    public void setRetencionMs(Long retencionMs) { this.retencionMs = retencionMs; }

    public Long getRetencionBytes() { return retencionBytes; }
    public void setRetencionBytes(Long retencionBytes) { this.retencionBytes = retencionBytes; }

    public String getCleanupPolicy() { return cleanupPolicy; }
    public void setCleanupPolicy(String cleanupPolicy) { this.cleanupPolicy = cleanupPolicy; }
}
