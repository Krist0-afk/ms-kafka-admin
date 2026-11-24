package com.pedidos360.kafkaadmin.service;

import com.pedidos360.kafkaadmin.dto.Requests;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

// Operaciones sobre el clúster con el AdminClient de Kafka
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);
    private static final long TIMEOUT_SEGUNDOS = 15;

    private final AdminClient admin;

    public KafkaAdminService(AdminClient admin) {
        this.admin = admin;
    }

    // ---------- Clúster ----------

    public Map<String, Object> cluster() {
        DescribeClusterResult cluster = admin.describeCluster();
        Collection<Node> nodos = esperar(cluster.nodes());
        Node controlador = esperar(cluster.controller());
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("clusterId", esperar(cluster.clusterId()));
        respuesta.put("controlador", controlador != null ? controlador.id() : null);
        respuesta.put("brokers", nodos.stream()
                .sorted(Comparator.comparingInt(Node::id))
                .map(n -> Map.of("id", n.id(), "host", n.host(), "puerto", n.port()))
                .toList());
        return respuesta;
    }

    // ---------- Tópicos ----------

    public List<Map<String, Object>> listarTopicos() {
        Set<String> nombres = esperar(admin.listTopics().names());
        Map<String, TopicDescription> descripciones = esperar(admin.describeTopics(nombres).allTopicNames());
        return descripciones.values().stream()
                .sorted(Comparator.comparing(TopicDescription::name))
                .map(d -> {
                    Map<String, Object> topico = new LinkedHashMap<>();
                    topico.put("nombre", d.name());
                    topico.put("particiones", d.partitions().size());
                    topico.put("replicas", d.partitions().isEmpty() ? 0 : d.partitions().get(0).replicas().size());
                    return topico;
                })
                .toList();
    }

    public Map<String, Object> describirTopico(String nombre) {
        TopicDescription descripcion = esperar(admin.describeTopics(List.of(nombre)).allTopicNames()).get(nombre);
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("nombre", descripcion.name());
        respuesta.put("particiones", descripcion.partitions().stream()
                .map(p -> {
                    Map<String, Object> particion = new LinkedHashMap<>();
                    particion.put("particion", p.partition());
                    particion.put("lider", p.leader() != null ? p.leader().id() : null);
                    particion.put("replicas", p.replicas().stream().map(Node::id).toList());
                    particion.put("enSincronia", p.isr().stream().map(Node::id).toList());
                    return particion;
                })
                .toList());
        respuesta.put("configs", configsPropias(nombre));
        return respuesta;
    }

    public void crearTopico(Requests.CrearTopico request) {
        NewTopic topico = new NewTopic(request.nombre(), request.particiones(), request.replicas());
        if (request.configs() != null && !request.configs().isEmpty()) {
            topico.configs(request.configs());
        }
        esperar(admin.createTopics(List.of(topico)).all());
        log.info("Tópico '{}' creado con {} particiones y {} réplicas", request.nombre(), request.particiones(), request.replicas());
    }

    public void eliminarTopico(String nombre) {
        esperar(admin.deleteTopics(List.of(nombre)).all());
        log.info("Tópico '{}' eliminado", nombre);
    }

    public void actualizarConfig(String nombre, Map<String, String> configs) {
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        List<AlterConfigOp> operaciones = configs.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        esperar(admin.incrementalAlterConfigs(Map.of(recurso, operaciones)).all());
        log.info("Configuración del tópico '{}' actualizada: {}", nombre, configs);
    }

    public void aumentarParticiones(String nombre, int total) {
        esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(total))).all());
        log.info("Tópico '{}' ahora tiene {} particiones", nombre, total);
    }

    // Solo las configuraciones definidas para el tópico (no las que hereda del broker)
    private Map<String, String> configsPropias(String nombre) {
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Config config = esperar(admin.describeConfigs(List.of(recurso)).all()).get(recurso);
        return config.entries().stream()
                .filter(e -> e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                .sorted(Comparator.comparing(ConfigEntry::name))
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value, (a, b) -> a, LinkedHashMap::new));
    }

    // ---------- Consumer groups ----------

    @SuppressWarnings("deprecation")
    public List<Map<String, Object>> listarGrupos() {
        Collection<ConsumerGroupListing> grupos = esperar(admin.listConsumerGroups().all());
        List<String> ids = grupos.stream().map(ConsumerGroupListing::groupId).sorted().toList();
        Map<String, ConsumerGroupDescription> descripciones = esperar(admin.describeConsumerGroups(ids).all());
        return ids.stream().map(id -> {
            ConsumerGroupDescription d = descripciones.get(id);
            Map<String, Object> grupo = new LinkedHashMap<>();
            grupo.put("grupo", id);
            grupo.put("estado", String.valueOf(d.groupState()));
            grupo.put("miembros", d.members().size());
            grupo.put("lagTotal", lagPorParticion(id).stream().mapToLong(p -> (long) p.get("lag")).sum());
            return grupo;
        }).toList();
    }

    // Offsets confirmados, último offset y lag de cada partición que consume el grupo
    public Map<String, Object> describirGrupo(String grupo) {
        ConsumerGroupDescription d = esperar(admin.describeConsumerGroups(List.of(grupo)).all()).get(grupo);
        List<Map<String, Object>> particiones = lagPorParticion(grupo);
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("grupo", grupo);
        respuesta.put("estado", String.valueOf(d.groupState()));
        respuesta.put("coordinador", d.coordinator() != null ? d.coordinator().id() : null);
        respuesta.put("miembros", d.members().stream()
                .map(m -> Map.of(
                        "consumidor", m.consumerId(),
                        "host", m.host(),
                        "particiones", m.assignment().topicPartitions().stream()
                                .map(TopicPartition::toString).sorted().toList()))
                .toList());
        respuesta.put("lagTotal", particiones.stream().mapToLong(p -> (long) p.get("lag")).sum());
        respuesta.put("particiones", particiones);
        return respuesta;
    }

    public void resetearOffsets(String grupo, Requests.ResetearOffsets request) {
        TopicDescription topico = esperar(admin.describeTopics(List.of(request.topico())).allTopicNames()).get(request.topico());
        OffsetSpec spec = "earliest".equals(request.posicion()) ? OffsetSpec.earliest() : OffsetSpec.latest();
        Map<TopicPartition, OffsetSpec> consulta = new LinkedHashMap<>();
        topico.partitions().forEach(p -> consulta.put(new TopicPartition(request.topico(), p.partition()), spec));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = esperar(admin.listOffsets(consulta).all());
        Map<TopicPartition, OffsetAndMetadata> nuevos = new LinkedHashMap<>();
        offsets.forEach((tp, info) -> nuevos.put(tp, new OffsetAndMetadata(info.offset())));
        // Kafka exige que el grupo no tenga consumidores activos para mover sus offsets
        esperar(admin.alterConsumerGroupOffsets(grupo, nuevos).all());
        log.info("Offsets del grupo '{}' en '{}' movidos a {}", grupo, request.topico(), request.posicion());
    }

    public void eliminarGrupo(String grupo) {
        esperar(admin.deleteConsumerGroups(List.of(grupo)).all());
        log.info("Consumer group '{}' eliminado", grupo);
    }

    private List<Map<String, Object>> lagPorParticion(String grupo) {
        Map<TopicPartition, OffsetAndMetadata> confirmados =
                esperar(admin.listConsumerGroupOffsets(grupo).partitionsToOffsetAndMetadata());
        if (confirmados.isEmpty()) {
            return List.of();
        }
        Map<TopicPartition, OffsetSpec> consulta = new LinkedHashMap<>();
        confirmados.keySet().forEach(tp -> consulta.put(tp, OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ultimos = esperar(admin.listOffsets(consulta).all());

        List<Map<String, Object>> resultado = new ArrayList<>();
        confirmados.entrySet().stream()
                .sorted(Comparator.comparing((Map.Entry<TopicPartition, OffsetAndMetadata> e) -> e.getKey().topic())
                        .thenComparingInt(e -> e.getKey().partition()))
                .forEach(e -> {
                    long confirmado = e.getValue() != null ? e.getValue().offset() : 0;
                    long ultimo = ultimos.get(e.getKey()).offset();
                    Map<String, Object> particion = new LinkedHashMap<>();
                    particion.put("topico", e.getKey().topic());
                    particion.put("particion", e.getKey().partition());
                    particion.put("offsetConfirmado", confirmado);
                    particion.put("ultimoOffset", ultimo);
                    particion.put("lag", Math.max(0, ultimo - confirmado));
                    resultado.add(particion);
                });
        return resultado;
    }

    // Espera el resultado de una operación del AdminClient con un tiempo máximo
    private static <T> T esperar(org.apache.kafka.common.KafkaFuture<T> futuro) {
        try {
            return futuro.get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new KafkaOperacionException(e.getCause());
        } catch (TimeoutException e) {
            throw new KafkaOperacionException(new IllegalStateException("Kafka no respondió en " + TIMEOUT_SEGUNDOS + " s"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaOperacionException(e);
        }
    }
}
