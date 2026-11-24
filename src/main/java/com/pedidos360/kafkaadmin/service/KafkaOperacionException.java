package com.pedidos360.kafkaadmin.service;

// Error devuelto por Kafka al ejecutar una operación de administración
public class KafkaOperacionException extends RuntimeException {

    public KafkaOperacionException(Throwable causa) {
        super(causa.getMessage(), causa);
    }
}
