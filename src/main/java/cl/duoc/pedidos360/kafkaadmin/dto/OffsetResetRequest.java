package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * DTO para reiniciar los offsets de un consumer group sobre un tópico.
 * El grupo debe estar sin miembros activos.
 */
public class OffsetResetRequest {

    @NotBlank(message = "El tópico es obligatorio")
    private String topico;

    @Pattern(regexp = "^(earliest|latest)$", message = "posicion debe ser earliest o latest")
    private String posicion = "earliest";

    public String getTopico() { return topico; }
    public void setTopico(String topico) { this.topico = topico; }

    public String getPosicion() { return posicion; }
    public void setPosicion(String posicion) { this.posicion = posicion; }
}
