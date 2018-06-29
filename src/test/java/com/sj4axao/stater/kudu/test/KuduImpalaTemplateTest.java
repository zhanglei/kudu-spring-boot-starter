package com.sj4axao.stater.kudu.test;

import com.sj4axao.stater.kudu.demo.DemoApplication;
import com.sj4axao.stater.kudu.demo.bean.User;
import com.sj4axao.stater.kudu.demo.dao.KuduImpalaTemplateDao;
import org.apache.kudu.client.KuduException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author: LiuJie
 * @version: 2018/6/27 17:02
 * @description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = DemoApplication.class)
public class KuduImpalaTemplateTest {
    @Autowired
    KuduImpalaTemplateDao kuduImpalaTemplateDao;

    @Test
    public void insertDefaultDB() throws KuduException {
        User user = new User();
        user.setId(3L);
        user.setName("jason");
        kuduImpalaTemplateDao.insertDefaultDB(user);
    }

    @Test
    public void tableList() throws KuduException {
        kuduImpalaTemplateDao.getTableList().forEach(s-> System.out.println(s));
    }
}
