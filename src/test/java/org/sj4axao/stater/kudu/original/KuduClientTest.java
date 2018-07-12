package org.sj4axao.stater.kudu.original;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sj4axao.stater.kudu.demo.DemoApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author: LiuJie
 * @version: 2018/7/11 15:39
 * @description: 对 kudu 原生api 进行测试
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = DemoApplication.class)
@Slf4j
public class KuduClientTest {
    @Autowired
    KuduClient kuduClient;
    @Test
    public void createTable() throws KuduException {
        List< ColumnSchema > columns = new LinkedList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("Clo1", Type.INT64).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("Clo2", Type.INT64).build());
        Schema schema = new Schema(columns);

        CreateTableOptions createTableOptions = new CreateTableOptions();
        createTableOptions.setRangePartitionColumns(Collections.singletonList("Clo1"));

        KuduTable kuduTable = kuduClient.createTable("kuduTable",schema,createTableOptions);
    }
    @Test
    public void getTables() throws KuduException {
        kuduClient.getTablesList().getTablesList().forEach(s -> System.out.println(s));
    }
    @Test
    public void getTable() throws KuduException {
        KuduTable kuduTable = kuduClient.openTable("kuduTable");
        kuduTable.getSchema().getColumns().forEach(s->log.info("clo={}",s));

    }
    @Test
    public void getCol() throws KuduException {
        KuduTable kuduTable = kuduClient.openTable("kuduTable");
        ColumnSchema clo1 = kuduTable.getSchema().getColumn("Clo1");
        log.info("col={}",clo1);
    }
    @Test
    public void delTable() throws KuduException {
        log.info("{}",kuduClient.deleteTable("wgj.apiTest"));
    }
}
