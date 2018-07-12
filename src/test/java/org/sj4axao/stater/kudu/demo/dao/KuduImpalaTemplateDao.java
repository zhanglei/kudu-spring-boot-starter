package org.sj4axao.stater.kudu.demo.dao;

import org.apache.kudu.client.KuduException;
import org.sj4axao.stater.kudu.client.KuduImpalaTemplate;
import org.sj4axao.stater.kudu.client.impl.PlainKuduImpalaTemplate;
import org.sj4axao.stater.kudu.demo.bean.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author: LiuJie
 * @version: 2018/6/27 16:19
 * @description:
 */
@Repository
public class KuduImpalaTemplateDao {
    @Autowired
    KuduImpalaTemplate kuduImpalaTemplate;

    public static final String TABLE_NAME = "user";

    public void insertDefaultDB(User user) throws KuduException {
        kuduImpalaTemplate.insert(TABLE_NAME,user);
    }

    public List<String> getTableList(){
        return kuduImpalaTemplate.getTablesList();
    }
}
