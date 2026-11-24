package com.pedidos360.kafkaadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Map;

// Tópicos declarados en application.yml (app.kafka.topicos)
@ConfigurationProperties(prefix = "app.kafka")
public record TopicosProperties(List<Topico> topicos) {

    public record Topico(String nombre, int particiones, short replicas, Map<String, String> configs) {
    }
}
