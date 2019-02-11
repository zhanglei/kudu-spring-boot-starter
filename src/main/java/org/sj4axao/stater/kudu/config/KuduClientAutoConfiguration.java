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
//@EnableConfigurationProperties(KuduProperties.class)
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
        KuduClient kuduClient;
        kuduClient = new KuduClient.KuduClientBuilder(masterAddr).build();
        /*if (StringUtils.isBlank(kuduProperties.getKerberosKeytab())) {
            logger.info("未配置kerberos...");
            kuduClient = new KuduClient.KuduClientBuilder(masterAddr).build();
        } else {
            logger.info("配置了kerberos...");
            initKerberosENV(kuduProperties);
            kuduClient = UserGroupInformation.getLoginUser().doAs(
                    (PrivilegedExceptionAction<KuduClient>) () -> new KuduClient.KuduClientBuilder(masterAddr).build()
            );
        }*/
        logger.info("kuduClient实例化结束");
        return kuduClient;
    }

    /**
     * 初始化访问Kerberos访问
     * 参考:https://mp.weixin.qq.com/s?__biz=MzI4OTY3MTUyNg==&mid=2247493634&idx=1&sn=e0c28169586a6afdb4f0c5e753473638&chksm=ec29380bdb5eb11d2e21d40a2be7c60602b2c30af11c32321e4bd505d4ee99e1ec0207965442&scene=21#wechat_redirect
     */
    /*public static void initKerberosENV(KuduProperties kuduProperties) {
        try {
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            if (kuduProperties.isKerberosDebug()) {
                System.setProperty("sun.security.krb5.debug", "true");
            }

            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            //configuration.addResource(KuduClientAutoConfiguration.class.getClass().getResourceAsStream("/hadoop-conf/core-site.xml"));
            //configuration.addResource(KuduClientAutoConfiguration.class.getClass().getResourceAsStream("/hadoop-conf/hdfs-site.xml"));
            configuration.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(configuration);

            UserGroupInformation.loginUserFromKeytab(kuduProperties.getKerberosPrincipal(), kuduProperties.getKerberosKeytab());
            logger.info("初始化kerberos环境完成,用户:{}" , UserGroupInformation.getCurrentUser());
        } catch (Exception e) {
            logger.error("初始化kerberos环境失败..", e);
        }
    }*/

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
