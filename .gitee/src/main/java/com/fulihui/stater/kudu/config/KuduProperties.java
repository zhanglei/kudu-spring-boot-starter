package com.fulihui.stater.kudu.config;

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
    private Long workerId;
}
