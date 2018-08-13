package org.sj4axao.stater.kudu.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sj4axao.stater.kudu.demo.DemoApplication;
import org.sj4axao.stater.kudu.demo.bean.User;
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
    @Autowired
    KuduTemplate kuduTemplate;

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
    public void delete() throws KuduException {
        User user = new User();
        user.setId(6L);
        user.setSex(4);
        kuduImpalaTemplate.delete("user",user);
    }


}