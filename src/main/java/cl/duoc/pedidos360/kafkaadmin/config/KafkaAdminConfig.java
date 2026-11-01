package cl.duoc.pedidos360.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    @Value("${pedidos360.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${pedidos360.kafka.request-timeout-ms}")
    private int requestTimeoutMs;

    @Bean(destroyMethod = "close")
    public AdminClient adminClient() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        return AdminClient.create(props);
    }
}
