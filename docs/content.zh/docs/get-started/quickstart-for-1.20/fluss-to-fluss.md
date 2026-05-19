# Streaming ELT 同步 Fluss 到 Fluss

这篇教程将展示如何基于 Flink CDC 快速构建 Fluss 到 Fluss 的 Streaming ELT 作业，实现整库实时同步和新增列自动同步的功能。 本教程的演示都将在 Flink CDC CLI 中进行，无需一行 Java/Scala 代码，也无需安装 IDE。

**使用场景**：跨数据库实时复制、数据备份、读写分离等。

## 准备阶段

准备一台已经安装了 Docker 的 Linux 或者 MacOS 电脑。

### 准备 Flink Standalone 集群

1.  下载 [Flink 1.20.3](https://archive.apache.org/dist/flink/flink-1.20.3/flink-1.20.3-bin-scala_2.12.tgz)，解压后得到 flink-1.20.3 目录。 使用下面的命令跳转至 Flink 目录下，并且设置 FLINK\_HOME 为 flink-1.20.3 所在目录。
    
    ```shell
    cd flink-1.20.3
    
    ```
    
2.  通过在 conf/config.yaml 配置文件追加下列参数开启 checkpoint，每隔 3 秒做一次 checkpoint。
    
    ```yaml
    execution:
      checkpointing:
        interval: 3s
    
    ```
    
3.  使用下面的命令启动 Flink 集群。
    

```shell
./bin/start-cluster.sh

```

启动成功的话，可以在 [http://localhost:8081/](http://localhost:8081/) 访问到 Flink Web UI。

多次执行 `start-cluster.sh` 可以拉起多个 TaskManager。

### 准备 Docker 环境

接下来的教程将以 `docker-compose` 的方式准备所需要的组件。

使用下面的内容创建一个 `docker-compose.yml` 文件：

```yaml
services:
  # Fluss 集群
  coordinator-server:
    image: apache/fluss:0.9.0-incubating
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123
        advertised.listeners: CLIENT://localhost:9123
        internal.listener.name: INTERNAL
        remote.data.dir: /tmp/fluss/remote-data
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_developer="developer-pass" ;
        super.users: User:admin
    ports:
      - "9123:9123"
  tablet-server:
    image: apache/fluss:0.9.0-incubating
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:9123
        advertised.listeners: CLIENT://localhost:9124
        internal.listener.name: INTERNAL
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_developer="developer-pass" ;
        super.users: User:admin
    ports:
      - "9124:9123"
  zookeeper:
    restart: always
    image: zookeeper:3.9.2

```

该 Docker Compose 中包含的容器有：

*   **Fluss**（coordinator-server, tablet-server, zookeeper）：同时作为源和目标的数据湖仓
    

在 `docker-compose.yml` 所在目录下执行下面的命令来启动本教程需要的组件：

```shell
docker-compose up -d

```

该命令将以 detached 模式自动启动 Docker Compose 配置中定义的所有容器。你可以通过 `docker ps` 来观察上述的容器是否正常启动了。

#### 在 Fluss 中准备源数据

需要通过 Flink SQL Client 来创建源库和源表。

1.  下载 [fluss-flink-1.20-0.9.0-incubating.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/0.9.0-incubating/fluss-flink-1.20-0.9.0-incubating.jar) 并放入 Flink 的 `lib` 目录。
    
2.  启动 Flink SQL Client：
    
    ```shell
    bin/sql-client.sh
    
    ```
    
3.  创建 Fluss Catalog：
    
    ```sql
    CREATE CATALOG fluss_catalog WITH (
        'type' = 'fluss',
        'bootstrap.servers' = 'localhost:9123',
        'client.security.protocol' = 'SASL',
        'client.security.sasl.mechanism' = 'PLAIN',
        'client.security.sasl.username' = 'admin',
        'client.security.sasl.password' = 'admin-pass'
    );
    
    USE CATALOG fluss_catalog;
    
    ```
    
4.  创建源数据库和表，并插入数据：
    
    ```sql
    create database source_db;
    
    -- 创建员工表
    CREATE TABLE fluss_catalog.source_db.employees (
        id INT PRIMARY KEY NOT ENFORCED,
        name STRING,
        age INT,
        salary DOUBLE
    );
    
    -- 创建订单表
    CREATE TABLE fluss_catalog.source_db.orders (
        id INT PRIMARY KEY NOT ENFORCED,
        product STRING,
        quantity INT,
        amount DOUBLE
    );
    
    -- 插入数据
    INSERT INTO fluss_catalog.source_db.employees VALUES
    (1, 'Alice', 28, 15000.0),
    (2, 'Bob', 35, 22000.0),
    (3, 'Charlie', 30, 18000.0);
    
    INSERT INTO fluss_catalog.source_db.orders VALUES
    (1, 'Laptop', 5, 49999.50),
    (2, 'Phone', 10, 8999.00),
    (3, 'Tablet', 3, 12500.00);
    
    ```
    
5.  验证数据已写入：
    

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

SELECT * FROM fluss_catalog.source_db.employees LIMIT 10;
 +----+---------+-----+---------+
 | id |    name | age |  salary |
 +----+---------+-----+---------+
 |  1 |   Alice |  28 | 15000.0 |
 |  2 |     Bob |  35 | 22000.0 |
 |  3 | Charlie |  30 | 18000.0 |
 +----+---------+-----+---------+

```

## 通过 Flink CDC CLI 提交任务

1.  下载[flink-cdc-3.6.0-1.20-bin.tar.gz](https://www.apache.org/dyn/closer.lua/flink/flink-cdc-3.6.0/flink-cdc-3.6.0-1.20-bin.tar.gz)，并解压得到目录
    
2.  下载下面列出的 connector 包，并且移动到 `lib` 目录下: 
    
    [请至钉钉文档查看附件《flink-cdc-pipeline-connector-fluss-3.7-SNAPSHOT.jar》。](https://alidocs.dingtalk.com/i/nodes/NZQYprEoWoxKPoqwCDv2z0gjV1waOeDk?iframeQuery=anchorId%3DX02moaunfafkvag96lqf6)
    
3.  编写任务配置 yaml 文件。 下面给出了一个整库同步的示例文件 `fluss-to-fluss.yaml`：
    
    ```yaml
    ################################################################################
    # Description: Sync Fluss source_db to Fluss replica_db
    ################################################################################
    source:
      type: fluss
      bootstrap.servers: localhost:9123
      subscriber.type: pattern
      subscriber.pattern: source_db\..*
      scan.startup.mode: full
      scan.discovery.interval: 10s
      properties.client.security.protocol: sasl
      properties.client.security.sasl.mechanism: PLAIN
      properties.client.security.sasl.username: developer
      properties.client.security.sasl.password: developer-pass
    
    sink:
      type: fluss
      bootstrap.servers: localhost:9123
      properties.client.security.protocol: sasl
      properties.client.security.sasl.mechanism: PLAIN
      properties.client.security.sasl.username: developer
      properties.client.security.sasl.password: developer-pass
    
    route:
      - source-table: source_db.\.*
        sink-table: replica_db.<>
        replace-symbol: <>
        description: route all tables from source_db to replica_db
    
    pipeline:
      name: Fluss to Fluss Pipeline
      parallelism: 2
      schema.change.behavior: LENIENT
    
    
    ```
    
    其中：
    
    *   source 中的 `database: source_db` 指定从 `source_db` 数据库读取，`table: .*` 匹配所有表。
        
    *   `scan.startup.mode: full` 表示首次启动时先进行全量快照，然后再消费增量日志。
        
    *   `scan.discovery.interval: 10s` 每隔 10 秒发现新创建的表或分区。
        
    *   route 中使用 `replace-symbol` 将源库 `source_db` 的所有表路由到目标库 `replica_db`。
        
    *   `schema.change.behavior: LENIENT` 开启宽松模式的 Schema 变更同步。
        
4.  最后，通过命令行提交任务到 Flink Standalone cluster
    

```shell
bash bin/flink-cdc.sh fluss-to-fluss.yaml --flink-home ~/software/flink-1.20.0 

```

提交成功后，返回信息如：

```shell
Pipeline has been submitted to cluster.
Job ID: ae30f4580f1918bebf16752d4963dc54
Job Description: Fluss to Fluss Pipeline

```

在 Flink Web UI，可以看到一个名为 `Fluss to Fluss Pipeline` 的任务正在运行。

![image.png](https://alidocs.oss-cn-zhangjiakou.aliyuncs.com/res/ABmOoWb2reaQeOaw/img/b6edc23e-c110-4c76-8f3e-a90a966b15af.png)

### 在 Fluss 中查询同步数据

回到 Flink SQL Client，查询目标库中的数据：

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

USE CATALOG fluss_catalog;
SHOW DATABASES;
+--------------+
| database name |
+--------------+
|        fluss |
|   source_db  |
|  replica_db  |
+--------------+

```

查询已同步的表：

```sql
SELECT * FROM `fluss_catalog`.`replica_db`.`employees` LIMIT 20;
 +----+---------+-----+---------+
 | id |    name | age |  salary |
 +----+---------+-----+---------+
 |  1 |   Alice |  28 | 15000.0 |
 |  2 |     Bob |  35 | 22000.0 |
 |  3 | Charlie |  30 | 18000.0 |
 +----+---------+-----+---------+

SELECT * FROM `fluss_catalog`.`replica_db`.`orders` LIMIT 20;
 +----+---------+----------+----------+
 | id | product | quantity |   amount |
 +----+---------+----------+----------+
 |  1 |  Laptop |        5 | 49999.50 |
 |  2 |   Phone |       10 |  8999.00 |
 |  3 |  Tablet |        3 | 12500.00 |
 +----+---------+----------+----------+

```

### 同步增量数据变更

在源库中插入、更新和删除数据：

```sql


-- 插入新数据
INSERT INTO fluss_catalog.source_db.employees VALUES (4, 'David', 32, 25000.0);

-- 更新已有数据
INSERT INTO fluss_catalog.source_db.orders VALUES (4, 'Sofa', 15, 16000.0);


```

查询目标库，可以看到变更已实时同步：

```sql
SELECT * FROM `fluss_catalog`.`replica_db`.`employees` LIMIT 20;
 +----+---------+-----+---------+
 | id |    name | age |  salary |
 +----+---------+-----+---------+
 |  1 |   Alice |  28 | 15000.0 |
 |  2 |     Bob |  35 | 22000.0 |
 |  3 | Charlie |  30 | 18000.0 |
 |  4 |   David |  32 | 25000.0 |
 +----+---------+-----+---------+

SELECT * FROM `fluss_catalog`.`replica_db`.`orders` LIMIT 20;
 ----+---------+----------+----------+
 | id | product | quantity |   amount |
 +----+---------+----------+----------+
 |  1 |  Laptop |        5 | 49999.50 |
 |  2 |   Phone |       10 |  8999.00 |
 |  3 |  Tablet |        3 | 12500.00 |
 |  4 |  Sofa   |       15 | 16000.00 |
 +----+---------+----------+----------+

```

### 同步表结构变更 — 新增列

在源库中为 `employees` 表新增列，并插入带有新列的数据：

```sql

ALTER TABLE fluss_catalog.source_db.employees ADD `email` STRING;

INSERT INTO fluss_catalog.source_db.employees VALUES (5, 'Eva', 27, 21000.0, 'eva@example.com');
INSERT INTO fluss_catalog.source_db.employees VALUES (6, 'Frank', 35, 28000.0, 'frank@example.com');

```

查询目标库，可以看到新列已自动创建，且已有数据的新列值为 NULL：

```sql
SELECT * FROM `fluss_catalog`.`replica_db`.`employees` LIMIT 20;
 +----+---------+-----+---------+-------------------+
 | id |    name | age |  salary |             email |
 +----+---------+-----+---------+-------------------+
 |   1 |   Alice |  28 | 15000.0 |            <NULL> |
 |  2 |     Bob |  35 | 22000.0 |            <NULL> |
 |  3 | Charlie |  30 | 18000.0 |            <NULL> |
 |  4 |   David |  32 | 25000.0 |            <NULL> |
 |  5 |     Eva |  27 | 21000.0 |   eva@example.com |
 |  6 |   Frank |  35 | 28000.0 | frank@example.com |
 +----+---------+-----+---------+-------------------+

```

## 插件化表订阅机制

Fluss CDC Source Connector 采用了**插件化的表订阅机制**，通过 Java SPI（ServiceLoader）动态加载不同的订阅策略，灵活决定需要同步哪些 Fluss 表。

### 核心架构

订阅机制由以下核心接口组成：

*   **`FlussSubscriber`**：定义了 `getSubscribedTablePaths(Connection connection)` 方法，返回需要订阅的表路径集合。
*   **`FlussSubscriberFactory`**：SPI 工厂接口，通过 `identifier()` 标识订阅类型，通过 `create(Configuration)` 创建具体的 `FlussSubscriber` 实例。

系统启动时，`FlussDataSourceFactory` 通过 `ServiceLoader` 加载所有注册的 `FlussSubscriberFactory`，根据配置中的 `subscriber.type` 值匹配对应工厂并创建订阅者。

### 内置订阅类型

| subscriber.type | 配置项 | 说明 |
| --- | --- | --- |
| `pattern`（默认） | `subscriber.pattern` | Java 正则表达式，匹配完全限定名 `database.tableName` 格式的表 |
| `fluss` | `subscriber.fluss` | 从 Fluss 主键表中读取订阅列表，该表唯一列存储完全限定表名 |

#### Pattern 订阅（正则匹配）

通过正则表达式匹配所有可见 Fluss 表的完全限定名（`database.tableName`）：

```yaml
source:
  type: fluss
  bootstrap.servers: localhost:9123
  subscriber.type: pattern
  subscriber.pattern: source_db\..*
```

正则表达式示例：

*   `source_db\..*` — 匹配 `source_db` 下所有表
*   `source_db\.orders_.*` — 匹配 `source_db` 下以 `orders_` 开头的表
*   `.*\.user_.*|.*\.order_.*` — 匹配所有库中以 `user_` 或 `order_` 开头的表

#### Fluss 表订阅（从 Fluss 主键表读取订阅列表）

从一张 Fluss 主键表中动态读取需要订阅的表列表。该订阅表必须满足：

1.  只有一列，类型为 `STRING`
2.  每行存储一个完全限定表名（`database.tableName` 格式）

```yaml
source:
  type: fluss
  bootstrap.servers: localhost:9123
  subscriber.type: fluss
  subscriber.fluss: meta_db.subscription
  subscriber.subscriber_table_limit: 1000
```

**示例：使用 Fluss 订阅表**

1.  首先在 Flink SQL Client 中创建订阅表：

    ```sql
    CREATE DATABASE meta_db;
    
    CREATE TABLE fluss_catalog.meta_db.subscription (
        table_name STRING PRIMARY KEY NOT ENFORCED
    );
    ```

2.  向订阅表中写入需要同步的目标表：

    ```sql
    INSERT INTO fluss_catalog.meta_db.subscription VALUES ('source_db.employees');
    INSERT INTO fluss_catalog.meta_db.subscription VALUES ('source_db.orders');
    ```

3.  在 CDC Pipeline YAML 中引用该订阅表：

    ```yaml
    source:
      type: fluss
      bootstrap.servers: localhost:9123
      subscriber.type: fluss
      subscriber.fluss: meta_db.subscription
      scan.startup.mode: full
      scan.discovery.interval: 10s
    ```

这种方式适合需要动态管理订阅列表的场景 — 只需修改订阅表中的数据，无需重启 CDC 任务即可在下一次表发现周期中自动生效。

### 自定义订阅插件

如果内置的 `pattern` 和 `fluss` 类型无法满足需求，可以通过以下步骤自定义订阅插件：

1.  **实现 `FlussSubscriber` 接口**：

    ```java
    public class MyCustomSubscriber implements FlussSubscriber {
        @Override
        public Set<TablePath> getSubscribedTablePaths(Connection connection) throws Exception {
            // 自定义逻辑：从外部系统获取表列表、基于标签过滤等
            return myDiscoveredTables;
        }
    }
    ```

2.  **实现 `FlussSubscriberFactory` 接口**：

    ```java
    public class MyCustomSubscriberFactory implements FlussSubscriberFactory {
        @Override
        public String identifier() {
            return "my-custom";  // 对应 subscriber.type 的值
        }
    
        @Override
        public FlussSubscriber create(Configuration subscriberConfig) {
            // 从 subscriberConfig 中读取自定义参数（键已去除 "subscriber." 前缀）
            return new MyCustomSubscriber(...);
        }
    
        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.singleton(MY_OPTION);
        }
    }
    ```

3.  **注册 SPI 服务**：在 `META-INF/services/org.apache.flink.cdc.connectors.fluss.source.subscriber.FlussSubscriberFactory` 文件中添加工厂类的全限定名。

4.  **使用自定义订阅**：

    ```yaml
    source:
      type: fluss
      bootstrap.servers: localhost:9123
      subscriber.type: my-custom
      subscriber.my-custom.param1: value1
    ```

## 环境清理

本教程结束后，在 `docker-compose.yml` 文件所在的目录下执行如下命令停止所有容器：

```shell
docker-compose down -v

```

在 Flink 所在目录 `flink-1.20.3` 下执行如下命令停止 Flink 集群：

```shell
./bin/stop-cluster.sh

```

{{< top >}}