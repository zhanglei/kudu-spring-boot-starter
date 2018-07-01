package org.sj4axao.stater.kudu.demo.dao;

import org.sj4axao.stater.kudu.client.KuduImpalaTemplate;
import org.sj4axao.stater.kudu.demo.bean.User;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kudu.client.KuduException;
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
    KuduImpalaTemplate kuduKuduImpalaTemplate;

    public static final String TABLE_NAME = "user";

    public void insertDefaultDB(User user) throws KuduException {
        CaseInsensitiveMap<String, Object> data = new CaseInsensitiveMap<>();
        data.put("id",user.getId());
        data.put("name",user.getName());
        // 不论是用 kudu 还是 impala 插入默认值都有效
//        data.put("sex",user.getSex());
        kuduKuduImpalaTemplate.insert(TABLE_NAME,data);
    }

    public List<String> getTableList(){
        return kuduKuduImpalaTemplate.getTablesList();
    }
}
