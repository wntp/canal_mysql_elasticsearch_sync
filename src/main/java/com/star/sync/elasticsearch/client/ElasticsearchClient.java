package com.star.sync.elasticsearch.client;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.net.InetAddress;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-25 17:32:00
 */
@Component
public class ElasticsearchClient implements DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchClient.class);
    private TransportClient transportClient;

    @Value("${elasticsearch.cluster.name}")
    private String clusterName;
    @Value("${elasticsearch.host}")
    private String host;
    @Value("${elasticsearch.port}")
    private String port;
    @Value("${elasticsearch.xpack}")
    private boolean xpack;
    @Value("${elasticsearch.userName}")
    private String userName;
    @Value("${elasticsearch.password}")
    private String password;
    @Value("${elasticsearch.sniff}")
    private boolean sniff;


    @Bean
    public TransportClient getTransportClient() throws Exception {
        if (xpack) {
            transportClient = new PreBuiltXPackTransportClient(Settings.builder()
                .put("cluster.name", clusterName)
                .put("xpack.security.user", String.format("%s:%s", userName, password))
                .put("client.transport.sniff", sniff)
                .build());
        } else {
            transportClient = new PreBuiltTransportClient(Settings.builder().put("cluster.name", clusterName)
                .put("client.transport.sniff", sniff)
                .build());
        }
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), Integer.valueOf(port)));
        logger.info("elasticsearch transportClient 连接成功");
        return transportClient;
    }

    @Override
    public void destroy() throws Exception {
        if (transportClient != null) {
            transportClient.close();
        }
    }
}
