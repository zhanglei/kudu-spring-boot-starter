package org.sj4axao.stater.kudu.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sj4axao.stater.kudu.client.impl.PlainKuduImpalaTemplate;
import org.sj4axao.stater.kudu.demo.DemoApplication;
import org.sj4axao.stater.kudu.demo.bean.User;
import org.sj4axao.stater.kudu.demo.dao.KuduImpalaTemplateDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author: LiuJie
 * @version: 2018/7/11 14:37
 * @description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = DemoApplication.class)
@Slf4j
public class KuduImpalaTemplateTest {

    @Autowired
    KuduImpalaTemplate kuduImpalaTemplate;

    @Test
    public void insertDefaultDB() throws KuduException {
        User user = new User();
        user.setId(6L);
        user.setName("jason");
        user.setSex(3);
        kuduImpalaTemplate.insert("user",user);
    }

    @Test
    public void tableList() {
        kuduImpalaTemplate.getTablesList().forEach(s-> System.out.println(s));
    }

    @Test
    public void getCols() throws KuduException {
        KuduTable user = kuduImpalaTemplate.getTable("user");
        for (ColumnSchema columnSchema : user.getSchema().getColumns()) {
            log.info("columnSchema={}",columnSchema);
        }
        Schema schema = user.getSchema();
        log.info("name={}",schema.getColumn("name"));
    }

    @Test
    public void getId(){
        System.out.println(kuduImpalaTemplate.getId());
    }

    @Test
    public void getTablesList() {
    }

    @Test
    public void getTable() {
    }

    @Test
    public void getTable1() {
    }

    @Test
    public void scanId() {
    }

    @Test
    public void scanId1() {
    }

    @Test
    public void createInsert() {
    }

    @Test
    public void createInsert1() {
    }

    @Test
    public void createUpdate() {
    }

    @Test
    public void createUpdate1() {
    }

    @Test
    public void createDelete() {
    }

    @Test
    public void createDelete1() {
    }

    @Test
    public void createUpsert() {
    }

    @Test
    public void apply() {
    }

    @Test
    public void apply1() {
    }

    @Test
    public void createUpsert1() {
    }

    @Test
    public void delete() {
    }

    @Test
    public void delete1() {
    }

    @Test
    public void insert() {
    }

    @Test
    public void insert1() {
    }

    @Test
    public void update() {
    }

    @Test
    public void update1() {
    }

    @Test
    public void upsert() {
    }

    @Test
    public void upsert1() {
    }

    @Test
    public void getDefaultDataBase() {
    }
}