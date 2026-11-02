package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.kafkaadmin.dto.TopicConfigUpdateRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicCreateRequest;
import cl.duoc.pedidos360.kafkaadmin.exception.KafkaAdminException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.Collection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaTopicServiceTest {

    @Mock
    private AdminClient adminClient;

    private TopicCreateRequest request() {
        TopicCreateRequest r = new TopicCreateRequest();
        r.setNombre("test.topic");
        r.setParticiones(3);
        r.setFactorReplicacion((short) 2);
        r.setRetencionMs(60000L);
        r.setCleanupPolicy("compact");
        return r;
    }

    @Test
    @SuppressWarnings("unchecked")
    void crearTopicoEnviaParticionesReplicasYConfiguracion() {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(adminClient.createTopics(anyCollection())).thenReturn(result);

        Map<String, Object> response = new KafkaTopicService(adminClient).crearTopico(request());

        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(adminClient).createTopics(captor.capture());
        NewTopic creado = captor.getValue().iterator().next();

        assertEquals("test.topic", creado.name());
        assertEquals(3, creado.numPartitions());
        assertEquals((short) 2, creado.replicationFactor());
        assertEquals("compact", creado.configs().get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("60000", creado.configs().get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals("test.topic", response.get("nombre"));
    }

    @Test
    void crearTopicoExistenteDevuelveConflict() {
        KafkaFuture<Void> fallo = new org.apache.kafka.common.internals.KafkaFutureImpl<>();
        ((org.apache.kafka.common.internals.KafkaFutureImpl<Void>) fallo)
                .completeExceptionally(new TopicExistsException("existe"));
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(fallo);
        when(adminClient.createTopics(anyCollection())).thenReturn(result);

        KafkaAdminException ex = assertThrows(KafkaAdminException.class,
                () -> new KafkaTopicService(adminClient).crearTopico(request()));

        assertEquals(HttpStatus.CONFLICT, ex.getStatus());
    }

    @Test
    void eliminarTopicoInexistenteDevuelveNotFound() {
        org.apache.kafka.common.internals.KafkaFutureImpl<Void> fallo =
                new org.apache.kafka.common.internals.KafkaFutureImpl<>();
        fallo.completeExceptionally(new UnknownTopicOrPartitionException("no existe"));
        DeleteTopicsResult result = mock(DeleteTopicsResult.class);
        when(result.all()).thenReturn(fallo);
        when(adminClient.deleteTopics(anyCollection())).thenReturn(result);

        KafkaAdminException ex = assertThrows(KafkaAdminException.class,
                () -> new KafkaTopicService(adminClient).eliminarTopico("fantasma"));

        assertEquals(HttpStatus.NOT_FOUND, ex.getStatus());
    }

    @Test
    void actualizarSinParametrosDevuelveBadRequest() {
        org.apache.kafka.clients.admin.DescribeTopicsResult describe =
                mock(org.apache.kafka.clients.admin.DescribeTopicsResult.class);
        org.apache.kafka.clients.admin.TopicDescription desc =
                new org.apache.kafka.clients.admin.TopicDescription("t", false, java.util.List.of());
        when(describe.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("t", desc)));
        when(adminClient.describeTopics(anyCollection())).thenReturn(describe);

        KafkaAdminException ex = assertThrows(KafkaAdminException.class,
                () -> new KafkaTopicService(adminClient).actualizarTopico("t", new TopicConfigUpdateRequest()));

        assertEquals(HttpStatus.BAD_REQUEST, ex.getStatus());
    }
}
