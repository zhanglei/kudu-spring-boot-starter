package org.sj4axao.stater.kudu.config;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.SessionConfiguration;
import org.sj4axao.stater.kudu.client.KuduImpalaTemplate;
import org.sj4axao.stater.kudu.client.KuduTemplate;
import org.sj4axao.stater.kudu.client.impl.PlainKuduImpalaTemplate;
import org.sj4axao.stater.kudu.client.impl.PlainKuduTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author: LiuJie
 * @version: 2018/4/19 9:30
 * @description:
 */
@Configuration
@EnableConfigurationProperties(KuduProperties.class)
public class KuduClientAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(KuduClientAutoConfiguration.class);

    @Bean
    @ConditionalOnProperty(value = "kudu.kudu-address")
    public KuduProperties kuduProperties() {
        return new KuduProperties();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnBean(KuduProperties.class)
    public KuduClient kuduClient(@Qualifier("kuduProperties") KuduProperties kuduProperties) throws IOException, InterruptedException {
        List<String> masterAddr = Arrays.asList(kuduProperties.getKuduAddress().split(","));
        logger.info("kuduClient实例化,servers:{}", masterAddr);
        KuduClient kuduClient = new KuduClient.KuduClientBuilder(masterAddr).build();
        logger.info("kuduClient实例化结束");
        return kuduClient;
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnBean(KuduClient.class)
    public KuduSession kuduSession(@Qualifier("kuduClient") KuduClient kuduClient) {
        KuduSession kuduSession = kuduClient.newSession();
        //
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        //缓存条数
        kuduSession.setMutationBufferSpace(10000);
        return kuduSession;
    }

    @Bean
    @ConditionalOnBean(KuduSession.class)
    public KuduTemplate KuduTemplate(KuduClient kuduClient, KuduSession kuduSession, KuduProperties kuduProperties) {
        return new PlainKuduTemplate(kuduClient, kuduSession, kuduProperties);
    }

    @Bean
    @ConditionalOnBean(KuduSession.class)
    public KuduImpalaTemplate KuduImpalaTemplate(KuduTemplate kuduTemplate) {
        return new PlainKuduImpalaTemplate(kuduTemplate);
    }


}
