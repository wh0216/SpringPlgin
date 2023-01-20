package com.wh.es;

import lombok.Value;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;

@Configuration
public class RestClientConfig extends AbstractElasticsearchConfiguration {
//    @Value("${es.clusterName}")
//    private String clusterName;
    //@Value("${es.servers}")
    private String servers="120.48.65.170";
    //@Value("${es.port}")
    private int port=9200;
    @Override
    @Bean
    public RestHighLevelClient elasticsearchClient() {
        final ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .connectedTo(servers + ":" + port)
                .build();
        return RestClients.create(clientConfiguration).rest();
    }
}
