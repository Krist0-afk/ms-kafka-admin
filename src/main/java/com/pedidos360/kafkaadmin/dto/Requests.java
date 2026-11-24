package com.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;

import java.util.Map;

// Cuerpos de las peticiones del administrador de Kafka
public final class Requests {

    public static final String NOMBRE_VALIDO = "^[a-zA-Z0-9._-]{3,200}$";
    public static final String MENSAJE_NOMBRE = "Nombre inválido: entre 3 y 200 caracteres (letras, números, '.', '-', '_')";

    private Requests() {
    }

    public record CrearTopico(
            @NotBlank @Pattern(regexp = NOMBRE_VALIDO, message = MENSAJE_NOMBRE) String nombre,
            @Min(1) @Max(50) int particiones,
            @Min(1) @Max(3) short replicas,
            Map<String, String> configs) {
    }

    // Solo las claves enviadas se modifican; el resto conserva su valor
    public record ActualizarConfig(@NotEmpty Map<String, String> configs) {
    }

    // Kafka solo permite aumentar las particiones, nunca reducirlas
    public record AumentarParticiones(@Min(2) @Max(50) int total) {
    }

    public record ResetearOffsets(
            @NotBlank @Pattern(regexp = NOMBRE_VALIDO, message = MENSAJE_NOMBRE) String topico,
            @NotBlank @Pattern(regexp = "earliest|latest", message = "La posición debe ser 'earliest' o 'latest'") String posicion) {
    }
}
