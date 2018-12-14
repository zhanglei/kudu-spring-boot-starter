package org.sj4axao.stater.kudu.client;

import app.util.PerformanceMonitor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.Upsert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sj4axao.stater.kudu.DemoApplication;
import org.sj4axao.stater.kudu.demo.bean.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
//        User user = new User();
//        user.setId(6L);
//        user.setName("jason");
//        user.setSex(3);


//        Map<String,Object> map = new HashMap<>();
//        map.put("AGE",12);
//        map.put("ID","S");
//        map.put("MARK","ADD BY");
//        map.put("NAME","ZSS ");

        PerformanceMonitor.begin("insert~~");
        List<Operation> upserts = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("AGE", i);
            map.put("ID", "S"+i);
            map.put("MARK", "ADD BY");
            map.put("NAME", "ZSS ");
            map.put("CREATE_DATE", "2018-12=21212X ");
            Upsert insert = kuduTemplate.createUpsert("impala::test_ogg.faith", map);
            upserts.add(insert);
        }


        kuduTemplate.apply(upserts);

        PerformanceMonitor.end();
        //kuduImpalaTemplate.insert("test_ogg.Faith1",faith1);
    }

    @Test
    public void tableList() {
        kuduImpalaTemplate.getTablesList().forEach(s -> System.out.println(s));
    }

    @Test
    public void getCols() throws KuduException {
        KuduTable user = kuduImpalaTemplate.getTable("user");
        for (ColumnSchema columnSchema : user.getSchema().getColumns()) {
            log.info("columnSchema={}", columnSchema);
        }
        Schema schema = user.getSchema();
        log.info("name={}", schema.getColumn("name"));
    }

    @Test
    public void delete() throws KuduException {
        User user = new User();
        user.setId(6L);
        user.setSex(4);
        kuduImpalaTemplate.delete("user", user);
    }


}
