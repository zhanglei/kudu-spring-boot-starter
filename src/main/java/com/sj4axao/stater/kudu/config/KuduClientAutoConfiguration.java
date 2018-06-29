package com.sj4axao.stater.kudu.config;

import com.sj4axao.stater.kudu.client.KuduImpalaTemplate;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

/**
 * @author: LiuJie
 * @version: 2018/4/19 9:30
 * @description:
 */
@Configuration
public class KuduClientAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(KuduClientAutoConfiguration.class);

    @Bean
    @ConditionalOnProperty(value = "kudu.kuduAddress")
    public KuduProperties kuduProperties(){
        return new KuduProperties();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnBean(KuduProperties.class)
    public KuduClient kuduClient(@Qualifier("kuduProperties")KuduProperties kuduProperties){
        List<String> masteraddr = Arrays.asList(kuduProperties.getKuduAddress().split(","));
        logger.info("kudu实例化,servers:{}",masteraddr);
        //创建kudu的数据库链接
        return  new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(30002).defaultOperationTimeoutMs(30001).build();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnBean(KuduClient.class)
    public KuduSession kuduSession(@Qualifier("kuduClient")KuduClient kuduClient){

        KuduSession kuduSession  = kuduClient.newSession();
        //
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        //缓存条数
        kuduSession.setMutationBufferSpace(10000);
        return  kuduSession;
    }

    @Bean
    @ConditionalOnBean(KuduSession.class)
    public KuduImpalaTemplate KuduImpalaTemplate(){
        return new KuduImpalaTemplate();
    }

}
