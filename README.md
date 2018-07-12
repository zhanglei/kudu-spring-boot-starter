# README

`kuduUtil`

---
**必要配置:**
```yml
kudu:
  # hostlist[]
  kuduAddress: 192.168.1.1,192.168.1.2,192.168.1.3
```
**非必要配置**
```yml
kudu:
  # 雪花算法节点id 取值 [0,1023] after v1.1.0 默认值35
  workerId: 0
  
  #使用KuduImpalaTemplate来操作Impala创建的Kudu表则最好设置默认DB，否则必须使用带dbName参数的重载方法
  default-data-base: test
```
**注入:**
```java
//todo
// 当然可以直接注入 KuduClient 和 KuduSession 来使用原生 api
```

---
versions
-
- 1.0.0-SNAPSHOT 1.0快照版，提供基本增删改查功能
- 1.1.0-SNAPSHOT 
> + 增加Long类型的ID生成器,雪花算法
> + 增加兼容多 DB 的重载方法，默认仍然是 ***
> + 增加 upsert 方法

- 2.0.0-SNAPSHOT 业务代码剥离
   + 分为 KuduTemplate 和 KuduImpalaTemplate 对外提供服务


---
other
-
- test DemoApplication中用到的库
```sql
CREATE TABLE test.user(
id BIGINT,
name STRING,
sex INT DEFAULT 1,
PRIMARY KEY(id)
)
PARTITION BY HASH(id) PARTITIONS 2
STORED AS KUDU;
```
