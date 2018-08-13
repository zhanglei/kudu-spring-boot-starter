# README

`kudu` `SpringBootStarter`

> 本项目是对 KuduClient 的封装，将常用操作封装为一个两个Template,用于在Spring配置文件中简单配置即可使用。

#### 使用流程：
1. 下载源码 maven 打包为jar包，deploy 至公司私服或 install 至本地仓库
2. pom 中引用
```xml
<dependency>
	<groupId>org.sj4axao</groupId>
	<artifactId>kudu-spring-boot-starter</artifactId>
	<!-- version 选择 源码中 pom 的最新版本 -->
	<version>***</version>
</dependency>		
```
3. 根据下述配置，在 yml 或 properties 中配置好
4. 根据注入下述注入方法在代码中直接注入即可使用

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
    /**
    * 用于操作 impala 创建的 kudu 表
    * impala 会生成特定的表名，并有DB的概念
    * 如：用impala在test库内创建user表 则kudu中真实的表名为 impala::test.user
    */
    @Autowired
    KuduImpalaTemplate kuduImpalaTemplate;
    /**
    * 用于操作 kudu 原生的表
    */
    @Autowired
    KuduTemplate kuduTemplate;
// 当然可以直接注入 KuduClient 和 KuduSession 来使用原生 api
```
---
tips：
-
1. insert、update、delete 字段的匹配模式是，找到传入的实体对象的所有 public get 方法，然后去掉get以后的方法名作为属性名，将属性名和 kudu 中的字段名全部去掉 "_"(下划线) 然后 变小写之后，进行比较一致的就认为是同一个字段，进行赋值
2. createTable 暂时没有封装，可以注入 kuduClient 调用 createTable 方法进行操作。
3. 当传入的data实体中值为 null 的字段会被跳过，也就是说无法将一个字段的值更新为 null

---
versions
-
- 1.0.0-SNAPSHOT 1.0快照版，提供基本增删改查功能
- 1.1.0-SNAPSHOT 
    + 增加Long类型的ID生成器,雪花算法
    + 增加兼容多 DB 的重载方法，默认仍然是 ***
    + 增加 upsert 方法

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
