package com.sj4axao.stater.kudu.demo.dao;

import com.sj4axao.stater.kudu.client.KuduImpalaTemplate;
import com.sj4axao.stater.kudu.demo.bean.User;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.ListTablesResponse;
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
        CaseInsensitiveMap<String, Object> data = new CaseInsensitiveMap<>();
        data.put("id",user.getId());
        data.put("name",user.getName());
        // 不论是用 kudu 还是 impala 插入默认值都有效
//        data.put("sex",user.getSex());
        kuduImpalaTemplate.insert(TABLE_NAME,data);
    }

    public List<String> getTableList(){
        return kuduImpalaTemplate.getTablesList();
    }
}
