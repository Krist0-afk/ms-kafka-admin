package com.pedidos360.kafkaadmin.controller;

import com.pedidos360.kafkaadmin.dto.Requests;
import com.pedidos360.kafkaadmin.service.KafkaAdminService;
import com.pedidos360.kafkaadmin.service.KafkaOperacionException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import java.util.List;
import java.util.Map;

import static com.pedidos360.kafkaadmin.dto.Requests.MENSAJE_NOMBRE;
import static com.pedidos360.kafkaadmin.dto.Requests.NOMBRE_VALIDO;

@RestController
@RequestMapping("/api/v1/kafka-admin")
@Validated
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("service", "ms-kafka-admin", "status", "UP");
    }

    @GetMapping("/cluster")
    public Map<String, Object> cluster() {
        return service.cluster();
    }

    // ---------- Tópicos ----------

    @GetMapping("/topics")
    public List<Map<String, Object>> listarTopicos() {
        return service.listarTopicos();
    }

    @GetMapping("/topics/{nombre}")
    public Map<String, Object> describirTopico(
            @PathVariable @Pattern(regexp = NOMBRE_VALIDO, message = MENSAJE_NOMBRE) String nombre) {
        return service.describirTopico(nombre);
    }

    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> crearTopico(@Valid @RequestBody Requests.CrearTopico request) {
        service.crearTopico(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "mensaje", "Tópico creado",
                "topico", request.nombre(),
                "particiones", request.particiones(),
                "replicas", request.replicas()));
    }

    @PutMapping("/topics/{nombre}/config")
    public Map<String, Object> actualizarConfig(
            @PathVariable @Pattern(regexp = NOMBRE_VALIDO, message = MENSAJE_NOMBRE) String nombre,
            @Valid @RequestBody Requests.ActualizarConfig request) {
        service.actualizarConfig(nombre, request.configs());
        return Map.of("mensaje", "Configuración actualizada", "topico", nombre, "configs", request.configs());
    }

    @PostMapping("/topics/{nombre}/partitions")
    public Map<String, Object> aumentarParticiones(
            @PathVariable @Pattern(regexp = NOMBRE_VALIDO, message = MENSAJE_NOMBRE) String nombre,
            @Valid @RequestBody Requests.AumentarParticiones request) {
        service.aumentarParticiones(nombre, request.total());
        return Map.of("mensaje", "Particiones aumentadas", "topico", nombre, "particiones", request.total());
    }

    @DeleteMapping("/topics/{nombre}")
    public Map<String, Object> eliminarTopico(
            @PathVariable @Pattern(regexp = NOMBRE_VALIDO, message = MENSAJE_NOMBRE) String nombre) {
        service.eliminarTopico(nombre);
        return Map.of("mensaje", "Tópico eliminado", "topico", nombre);
    }

    // ---------- Consumer groups ----------

    @GetMapping("/consumer-groups")
    public List<Map<String, Object>> listarGrupos() {
        return service.listarGrupos();
    }

    @GetMapping("/consumer-groups/{grupo}")
    public Map<String, Object> describirGrupo(
            @PathVariable @Pattern(regexp = NOMBRE_VALIDO, message = MENSAJE_NOMBRE) String grupo) {
        return service.describirGrupo(grupo);
    }

    @PostMapping("/consumer-groups/{grupo}/reset")
    public Map<String, Object> resetearOffsets(
            @PathVariable @Pattern(regexp = NOMBRE_VALIDO, message = MENSAJE_NOMBRE) String grupo,
            @Valid @RequestBody Requests.ResetearOffsets request) {
        service.resetearOffsets(grupo, request);
        return Map.of("mensaje", "Offsets movidos", "grupo", grupo, "topico", request.topico(), "posicion", request.posicion());
    }

    @DeleteMapping("/consumer-groups/{grupo}")
    public Map<String, Object> eliminarGrupo(
            @PathVariable @Pattern(regexp = NOMBRE_VALIDO, message = MENSAJE_NOMBRE) String grupo) {
        service.eliminarGrupo(grupo);
        return Map.of("mensaje", "Consumer group eliminado", "grupo", grupo);
    }

    // ---------- Errores ----------

    @ExceptionHandler(KafkaOperacionException.class)
    public ResponseEntity<Map<String, String>> errorKafka(KafkaOperacionException ex) {
        Throwable causa = ex.getCause();
        HttpStatus estado = HttpStatus.BAD_GATEWAY;
        if (causa instanceof UnknownTopicOrPartitionException || causa instanceof GroupIdNotFoundException) {
            estado = HttpStatus.NOT_FOUND;
        } else if (causa instanceof TopicExistsException || causa instanceof GroupNotEmptyException) {
            estado = HttpStatus.CONFLICT;
        } else if (causa instanceof InvalidConfigurationException || causa instanceof InvalidPartitionsException
                || causa instanceof InvalidReplicationFactorException) {
            estado = HttpStatus.BAD_REQUEST;
        }
        return ResponseEntity.status(estado).body(Map.of("error", String.valueOf(ex.getMessage())));
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HandlerMethodValidationException.class})
    public ResponseEntity<Map<String, String>> errorValidacion(Exception ex) {
        String mensaje = ex instanceof MethodArgumentNotValidException m && m.getBindingResult().getFieldError() != null
                ? m.getBindingResult().getFieldError().getField() + ": " + m.getBindingResult().getFieldError().getDefaultMessage()
                : "Parámetros inválidos";
        if (ex instanceof HandlerMethodValidationException h && !h.getAllErrors().isEmpty()) {
            mensaje = h.getAllErrors().get(0).getDefaultMessage();
        }
        return ResponseEntity.badRequest().body(Map.of("error", mensaje));
    }
}
