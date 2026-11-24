package com.pedidos360.kafkaadmin.controller;

import com.pedidos360.kafkaadmin.dto.Requests;
import com.pedidos360.kafkaadmin.service.KafkaAdminService;
import com.pedidos360.kafkaadmin.service.KafkaOperacionException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaAdminControllerTest {

    @Mock
    private KafkaAdminService service;

    @InjectMocks
    private KafkaAdminController controller;

    @Test
    void crearTopicoResponde201() {
        var request = new Requests.CrearTopico("pedidos360.prueba", 3, (short) 3, Map.of("retention.ms", "86400000"));

        var respuesta = controller.crearTopico(request);

        assertThat(respuesta.getStatusCode().value()).isEqualTo(201);
        verify(service).crearTopico(request);
    }

    @Test
    void erroresDeKafkaSeTraducenACodigosHttp() {
        assertThat(controller.errorKafka(new KafkaOperacionException(new TopicExistsException("existe"))).getStatusCode().value()).isEqualTo(409);
        assertThat(controller.errorKafka(new KafkaOperacionException(new UnknownTopicOrPartitionException("no existe"))).getStatusCode().value()).isEqualTo(404);
        assertThat(controller.errorKafka(new KafkaOperacionException(new InvalidPartitionsException("menos"))).getStatusCode().value()).isEqualTo(400);
        assertThat(controller.errorKafka(new KafkaOperacionException(new GroupNotEmptyException("activo"))).getStatusCode().value()).isEqualTo(409);
    }

    @Test
    void actualizarConfigDelegaEnElServicio() {
        controller.actualizarConfig("pedidos360.pagos", new Requests.ActualizarConfig(Map.of("retention.ms", "604800000")));

        verify(service).actualizarConfig("pedidos360.pagos", Map.of("retention.ms", "604800000"));
    }
}
