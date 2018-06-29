package com.sj4axao.stater.kudu.client;

import com.sj4axao.stater.kudu.config.KuduProperties;
import com.sj4axao.stater.kudu.utils.IdGenerator;
import com.sj4axao.stater.kudu.utils.KuduUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.client.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: LiuJie
 * @version: 2018/6/29 16:56
 * @description:
 */
@Slf4j
public class BaseTemplate {
    @Autowired
    protected KuduSession kuduSession;
    @Autowired
    protected KuduClient kuduClient;
    @Autowired
    protected KuduProperties kuduProperties;

    protected IdGenerator idGenerator;
    protected KuduUtil kuduUtil;
    protected static Map<String, KuduTable> tables ;

    protected void init(){
        tables = new HashMap<>();
        kuduUtil = new KuduUtil();
        // 初始化 id 生成器
        Long wordId = kuduProperties.getWorkerId();
        log.info("workId = {}",wordId);
        idGenerator = new IdGenerator(wordId);
    }

    /**
     * @return 不重复的 Long 类型 id
     */
    public long getId(){
        return idGenerator.nextId();
    }


    /**
     * 获取table列表
     * 数据库是Impala的定义，kudu没有的，只是表名不同，
     * 所以
     * @return 所有库的所有表，即 kudu 中的所有表
     */
    public List<String> getTablesList(){
        try {
            return kuduClient.getTablesList().getTablesList();
        } catch (KuduException e) {
            e.printStackTrace();
            return null;
        }
    }
}
