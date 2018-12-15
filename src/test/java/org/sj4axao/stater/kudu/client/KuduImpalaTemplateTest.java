package org.sj4axao.stater.kudu.client;

import app.util.PerformanceMonitor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.Update;
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


//        for (int i = 0; i < 100; i++) {
//            Map<String, Object> map = new HashMap<>();
//            map.put("AGE", i);
//            map.put("ID", "TS" + i);
//            map.put("MARK", "ADD BY");
//            map.put("NAME", "ZSS ");
//            map.put("CREATE_DATE", "2018-12=21212X ");
//            Insert insert = kuduTemplate.createInsert("impala::test_ogg.faith", map);
//            upserts.add(insert);
//        }
//        kuduTemplate.apply(upserts);
//
//        for(int j=5000;j>50;j=j-50) {
//            upserts.clear();
//            for (int i = 0; i < 100000; i++) {
//                Map<String, Object> map = new HashMap<>();
//                map.put("AGE", i);
//                map.put("ID", j+ "S" + i);
//                map.put("MARK", "ADD BY");
//                map.put("NAME", "ZSS ");
//                map.put("CREATE_DATE", "2018-12=21212X ");
//                Insert insert = kuduTemplate.createInsert("impala::test_ogg.faith", map);
//                upserts.add(insert);
//            }
//            List<OperationResponse> responses= kuduTemplate.apply(upserts,j);
//        }

        for (int i = 0; i < 100; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("AGE", i);
            map.put("ID", "TS" + i);
            map.put("MARK", "ADD BY");
            map.put("NAME", "ZSS ");
            map.put("CREATE_DATE", "2018-12=21212X ");
            Update insert = kuduTemplate.createUpdate("impala::test_ogg.faith", map);
            upserts.add(insert);
        }
        kuduTemplate.apply(upserts);

        for(int j=5000;j>50;j=j-50) {
            upserts.clear();
            for (int i = 0; i < 100000; i++) {
                Map<String, Object> map = new HashMap<>();
                map.put("AGE", i);
                map.put("ID", j+ "S" + i);
                map.put("MARK", "ADD BY");
                map.put("NAME", "ZSS ");
                map.put("CREATE_DATE", "2018-");
                Delete insert = kuduTemplate.createDelete("impala::test_ogg.faith", map);
                upserts.add(insert);
            }
            List<OperationResponse> responses= kuduTemplate.apply(upserts,j);
        }

        PerformanceMonitor.end();
        //kuduImpalaTemplate.insert("test_ogg.Faith1",faith1);
    }

    @Test
    public void tableList() {
        kuduImpalaTemplate.getTablesList().forEach(s -> System.out.println(s));
    }

    @Test
    public void getCols() throws KuduException {
        KuduTable user = kuduImpalaTemplate.getTable("test_ogg","faith");
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
