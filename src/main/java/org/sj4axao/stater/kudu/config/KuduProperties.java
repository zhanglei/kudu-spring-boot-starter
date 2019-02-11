package org.sj4axao.stater.kudu.config;

import com.sun.istack.internal.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author: LiuJie
 * @version: 2018/4/18 18:02
 * @description:
 */
@Data
@ConfigurationProperties(prefix = "kudu")
public class KuduProperties {
    public static final Long DEFAULT_WORK_ID = 35L; // 此处的 35 没有什么特殊意义，就是随便给个默认值，任性没办法
    @NotNull
    private String kuduAddress;
    private Long workerId = DEFAULT_WORK_ID;

    // 对于操作 impala 创建的 kudu 表，存在 DB 概念
    private String defaultDataBase;

    /**
     *  keytab文件
     */
    private String kerberosKeytab;
    /**
     * keytab文件对应的用户
     */
    private String kerberosPrincipal;
    /**
     * krb5是否开启debug
     */
    private boolean kerberosDebug = false;
}
