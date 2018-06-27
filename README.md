# README

`kuduUtil`

---
**pom：**
```xml
<dependency>
    <groupId>com.fulihui</groupId>
    <artifactId>kudu-spring-boot-starter</artifactId>
</dependency>
```
**必要配置:**
```yml
kudu:
  kuduAddress: 192.168.1.131,192.168.1.132,192.168.1.111  hostlist[]
  workerId: 0 节点id [0,1023] after 1.1.0
```
**注入:**
```java
@Autowired
private KuduUtil kuduUtil;
// 当然可以直接注入 KuduClient 和 KuduSession 来使用原生 api
```

---
versions
-
- 1.0.0-SNAPSHOT 1.0快照版，提供基本增删改查功能
- 1.1.0-SNAPSHOT 
> + 增加Long类型的ID生成器
> + 增加兼容多 DB 的重载方法，默认仍然是 ***
> + 增加 upsert 方法

- 2.0.0-SNAPSHOT 准备分享