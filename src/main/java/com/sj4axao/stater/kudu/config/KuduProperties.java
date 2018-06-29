package com.sj4axao.stater.kudu.config;

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
    private String kuduAddress;
    private Long workerId = 35L; // 此处的 35 没有什么特殊意义，就是随便给个默认值，任性没办法
    private String defaultDataBase;
}
