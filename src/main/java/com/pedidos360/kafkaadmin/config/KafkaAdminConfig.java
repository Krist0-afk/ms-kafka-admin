package com.pedidos360.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaAdminConfig {

    // KafkaAdmin crea estos tópicos al arrancar si no existen (y no toca los que ya están).
    // Así la configuración de cada tópico queda versionada en el código.
    @Bean
    public KafkaAdmin.NewTopics topicosPedidos360(TopicosProperties properties) {
        NewTopic[] topicos = properties.topicos().stream()
                .map(t -> TopicBuilder.name(t.nombre())
                        .partitions(t.particiones())
                        .replicas(t.replicas())
                        .configs(t.configs())
                        .build())
                .toArray(NewTopic[]::new);
        return new KafkaAdmin.NewTopics(topicos);
    }

    // Cliente de administración para los endpoints (usa la misma configuración que KafkaAdmin)
    @Bean(destroyMethod = "close")
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
}
