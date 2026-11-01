package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.kafkaadmin.dto.TopicConfigUpdateRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicCreateRequest;
import cl.duoc.pedidos360.kafkaadmin.exception.KafkaAdminException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaTopicService {

    private final AdminClient adminClient;

    public KafkaTopicService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    // ---------------------------------------------------------
    // Utilidad: resolver un KafkaFuture traduciendo errores a HTTP
    // ---------------------------------------------------------
    static <T> T await(KafkaFuture<T> future, String contexto) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Operación interrumpida: " + contexto, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof UnknownTopicOrPartitionException) {
                throw new KafkaAdminException(HttpStatus.NOT_FOUND,
                        "El tópico no existe", cause);
            }
            if (cause instanceof TopicExistsException) {
                throw new KafkaAdminException(HttpStatus.CONFLICT,
                        "El tópico ya existe", cause);
            }
            if (cause instanceof org.apache.kafka.common.errors.InvalidRequestException
                    || cause instanceof org.apache.kafka.common.errors.InvalidConfigurationException
                    || cause instanceof org.apache.kafka.common.errors.InvalidPartitionsException
                    || cause instanceof org.apache.kafka.common.errors.InvalidReplicationFactorException) {
                throw new KafkaAdminException(HttpStatus.BAD_REQUEST,
                        "Solicitud inválida: " + contexto, cause);
            }
            throw new KafkaAdminException(HttpStatus.BAD_GATEWAY,
                    "Error al comunicarse con Kafka: " + contexto, cause);
        }
    }

    // ---------------------------------------------------------
    // Crear
    // ---------------------------------------------------------
    public Map<String, Object> crearTopico(TopicCreateRequest request) {

        NewTopic newTopic = new NewTopic(
                request.getNombre(),
                request.getParticiones(),
                request.getFactorReplicacion());

        Map<String, String> configs = new LinkedHashMap<>();
        if (request.getRetencionMs() != null) {
            configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(request.getRetencionMs()));
        }
        if (request.getRetencionBytes() != null) {
            configs.put(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(request.getRetencionBytes()));
        }
        if (request.getCleanupPolicy() != null && !request.getCleanupPolicy().isBlank()) {
            configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, request.getCleanupPolicy());
        }
        newTopic.configs(configs);

        await(adminClient.createTopics(Collections.singleton(newTopic)).all(),
                "crear tópico " + request.getNombre());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("estado", "Tópico creado correctamente");
        response.put("nombre", request.getNombre());
        response.put("particiones", request.getParticiones());
        response.put("factorReplicacion", request.getFactorReplicacion());
        response.put("configuracion", configs);
        return response;
    }

    // ---------------------------------------------------------
    // Listar
    // ---------------------------------------------------------
    public List<String> listarTopicos() {
        Set<String> nombres = await(adminClient.listTopics().names(), "listar tópicos");
        return nombres.stream().sorted().toList();
    }

    // ---------------------------------------------------------
    // Detalle (particiones + configuración)
    // ---------------------------------------------------------
    public Map<String, Object> describirTopico(String nombre) {

        TopicDescription description = await(
                adminClient.describeTopics(Collections.singleton(nombre)).allTopicNames(),
                "describir tópico " + nombre).get(nombre);

        List<Map<String, Object>> particiones = new ArrayList<>();
        description.partitions().forEach(p -> {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("particion", p.partition());
            info.put("lider", p.leader() != null ? p.leader().id() : null);
            info.put("replicas", p.replicas().stream().map(n -> n.id()).toList());
            info.put("isr", p.isr().stream().map(n -> n.id()).toList());
            particiones.add(info);
        });

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("nombre", description.name());
        response.put("interno", description.isInternal());
        response.put("totalParticiones", description.partitions().size());
        response.put("factorReplicacion",
                description.partitions().isEmpty() ? 0 : description.partitions().get(0).replicas().size());
        response.put("particiones", particiones);
        response.put("configuracion", obtenerConfiguracion(nombre));
        return response;
    }

    public Map<String, String> obtenerConfiguracion(String nombre) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Config config = await(
                adminClient.describeConfigs(Collections.singleton(resource)).all(),
                "consultar configuración de " + nombre).get(resource);

        Map<String, String> result = new LinkedHashMap<>();
        for (ConfigEntry entry : config.entries()) {
            // Solo configuraciones relevantes + las sobreescritas explícitamente
            if (!entry.isDefault() || esConfigBasica(entry.name())) {
                result.put(entry.name(), entry.value());
            }
        }
        return result;
    }

    private boolean esConfigBasica(String name) {
        return TopicConfig.RETENTION_MS_CONFIG.equals(name)
                || TopicConfig.RETENTION_BYTES_CONFIG.equals(name)
                || TopicConfig.CLEANUP_POLICY_CONFIG.equals(name)
                || TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG.equals(name)
                || TopicConfig.MAX_MESSAGE_BYTES_CONFIG.equals(name);
    }

    // ---------------------------------------------------------
    // Modificar configuración / particiones
    // ---------------------------------------------------------
    public Map<String, Object> actualizarTopico(String nombre, TopicConfigUpdateRequest request) {

        // Verifica que exista (lanza 404 si no)
        TopicDescription actual = await(
                adminClient.describeTopics(Collections.singleton(nombre)).allTopicNames(),
                "describir tópico " + nombre).get(nombre);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("nombre", nombre);

        List<AlterConfigOp> ops = new ArrayList<>();
        if (request.getRetencionMs() != null) {
            ops.add(set(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(request.getRetencionMs())));
        }
        if (request.getRetencionBytes() != null) {
            ops.add(set(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(request.getRetencionBytes())));
        }
        if (request.getCleanupPolicy() != null && !request.getCleanupPolicy().isBlank()) {
            ops.add(set(TopicConfig.CLEANUP_POLICY_CONFIG, request.getCleanupPolicy()));
        }

        if (!ops.isEmpty()) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
            Map<ConfigResource, Collection<AlterConfigOp>> cambios =
                    Collections.singletonMap(resource, ops);
            await(adminClient.incrementalAlterConfigs(cambios).all(),
                    "actualizar configuración de " + nombre);
            response.put("configuracionActualizada", true);
        }

        if (request.getParticiones() != null) {
            int actuales = actual.partitions().size();
            if (request.getParticiones() <= actuales) {
                throw new KafkaAdminException(HttpStatus.BAD_REQUEST,
                        "Solo se pueden aumentar las particiones (actuales: " + actuales + ")");
            }
            await(adminClient.createPartitions(
                            Collections.singletonMap(nombre, NewPartitions.increaseTo(request.getParticiones())))
                    .all(), "aumentar particiones de " + nombre);
            response.put("particionesAumentadasA", request.getParticiones());
        }

        if (ops.isEmpty() && request.getParticiones() == null) {
            throw new KafkaAdminException(HttpStatus.BAD_REQUEST,
                    "No se indicó ningún parámetro a modificar");
        }

        response.put("estado", "Tópico actualizado correctamente");
        return response;
    }

    private AlterConfigOp set(String key, String value) {
        return new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET);
    }

    // ---------------------------------------------------------
    // Eliminar
    // ---------------------------------------------------------
    public Map<String, Object> eliminarTopico(String nombre) {
        await(adminClient.deleteTopics(Collections.singleton(nombre)).all(),
                "eliminar tópico " + nombre);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("estado", "Tópico eliminado correctamente");
        response.put("nombre", nombre);
        return response;
    }
}
