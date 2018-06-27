package com.fulihui.stater.kudu.client;

import com.fulihui.stater.kudu.config.KuduProperties;
import com.fulihui.stater.kudu.helper.IdGenerator;
import com.fulihui.stater.kudu.helper.KuduUtilHelper;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: LiuJie
 * @version: 2018/4/19 11:11
 * @description: 对外暴露的功能，适用于 impala 建的kudu表
 */

public class KuduUtil {

    private static final Logger logger = LoggerFactory.getLogger(KuduUtil.class);

    @Autowired
    private KuduSession kuduSession;
    @Autowired
    private KuduClient kuduClient;
    @Autowired
    private KuduProperties kuduProperties;

    private static final String TABLE_PREFIX = "impala::";
    private static final String DEFAULT_DB_NAME = "wgj";
    private static final String DOT = ".";

    private static Map<String, KuduTable> tables ;
    private KuduUtilHelper kuduUtilHelper ;
    private IdGenerator idGenerator;

    public KuduUtil(){
        tables = new HashMap<>();
        kuduUtilHelper = new KuduUtilHelper();
    }

    @PostConstruct
    public void init(){
        // 初始化 id 生成器
        Long wordId = kuduProperties.getWorkerId();
        if(wordId==null){
            wordId = 35L;  // 此处的 35 没有什么特殊意义，就是随便给个默认值，任性没办法
        }
        logger.info("workId = {}",wordId);
        idGenerator = new IdGenerator(wordId);
    }

    /**
     * 返回 不重复的 Long 类型 id
     * @return
     */
    public long getId(){
        return idGenerator.nextId();
    }

    /**
     * 获取table列表
     * 数据库是Impala的定义，kudu没有的，只是表名不同，
     * 所以
     * @return 所有库的所有表
     */
    public ListTablesResponse getTables(){
        try {
            return kuduClient.getTablesList();
        } catch (KuduException e) {
            e.printStackTrace();
            return null;
        }

    }

    public KuduTable getTable(String tableName) throws KuduException {
        return getTable(DEFAULT_DB_NAME,tableName);
    }

    public KuduTable getTable(String dbName ,String tableName) throws KuduException {
        String finalTableName = TABLE_PREFIX + dbName + DOT + tableName;
        KuduTable table = tables.get(finalTableName);
        if (table == null) {
            table = kuduClient.openTable(finalTableName);
            tables.put(tableName, table);
        }
        return table;
    }

    public Long scanId(String table,Map<String,String> args) throws KuduException {
        return scanId(DEFAULT_DB_NAME,table,args);
    }
    public Long scanId(String dbName,String table,Map<String,String> args) throws KuduException {

        if(CollectionUtils.isEmpty(args)){
            return null;
        }

        KuduTable ktable = getTable(dbName,table);
        KuduScanner.KuduScannerBuilder scanner = kuduClient.newScannerBuilder(ktable);

        //指定kudu返回字段：默认只返回id
        List<String> projectColumns = new ArrayList<>(1);
        projectColumns.add("id");

        //组装条件
        args.forEach((k,v) -> {
            KuduPredicate predicate = KuduPredicate.newComparisonPredicate(ktable.getSchema().getColumn(k.toLowerCase()),
                    KuduPredicate.ComparisonOp.EQUAL, v);
            scanner.addPredicate(predicate);
        });

        KuduScanner build = scanner.build();
        while (build.hasMoreRows()) {
            RowResultIterator results = build.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                return result.getLong(0);
            }
        }
        return null;
    }

    /* ***************************** 构建operation对象 *******************************************************************/

    public Insert createInsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        return createInsert(DEFAULT_DB_NAME,table,data);
    }

    public Insert createInsert(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = getTable(dbName,table);
        Insert insert = ktable.newInsert();
        kuduUtilHelper.fillRow(data, ktable, insert);
        return insert;
    }

    public Update createUpdate(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        return createUpdate(DEFAULT_DB_NAME,table,data);
    }

    /**
     * 构建并返回一个删除操作，一般用于批量 apply
     * @param dbName
     * @param table
     * @param data 一定要有主键字段
     * @return
     * @throws KuduException
     */
    public Update createUpdate(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = getTable(dbName,table);
        Update update = ktable.newUpdate();
        kuduUtilHelper.fillRow(data, ktable, update);
        return update;
    }

    /**
     * 构建删除
     * @param table String tabelName
     * @param data <column,value>删除条件 必须要有主键，存在其他条件的时候 and 关系，不符合就不删除
     * @return
     * @throws KuduException
     */
    public Delete createDelete(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        return createDelete(DEFAULT_DB_NAME,table,data);
    }
    public Delete createDelete(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = getTable(dbName,table);
        Delete delete = ktable.newDelete();
        kuduUtilHelper.fillRow(data, ktable, delete);
        return delete;
    }

    /**
     * @param table
     * @param data 必须包含 主键 和 not null 字段
     * @return
     * @throws KuduException
     */
    public Upsert createUpsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        return createUpsert(DEFAULT_DB_NAME,table,data);
    }
    public Upsert createUpsert(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = getTable(dbName,table);
        Upsert upsert = ktable.newUpsert();
        kuduUtilHelper.fillRow(data, ktable, upsert);
        return upsert;
    }

    /* *********************** 单条操作 **********************************************************************************/

    /**
     * 单条删除
     * @param table
     * @param data
     * @throws KuduException
     */
    public void delete(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        delete(DEFAULT_DB_NAME,table,data);
    }
    public void delete(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        Delete delete = createDelete(dbName,table, data);
        kuduSession.apply(delete);
        kuduSession.flush();
    }

    /**
     * 单条插入
     * @param table
     * @param data
     * @throws KuduException
     */
    public void insert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        insert(DEFAULT_DB_NAME,table,data);
    }
    public void insert(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        Insert insert = createInsert(dbName,table, data);
        kuduSession.apply(insert);
        kuduSession.flush();
    }

    /**
     * 单条更新
     * @param table
     * @param data
     * @throws KuduException
     */
    public void update(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        update(DEFAULT_DB_NAME,table,data);
    }
    public void update(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        Update update = createUpdate(dbName,table, data);
        kuduSession.apply(update);
        kuduSession.flush();
    }

    public void upsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        upsert(DEFAULT_DB_NAME,table,data);
    }
    public void upsert(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        Upsert upsert = createUpsert(dbName,table, data);
        kuduSession.apply(upsert);
        kuduSession.flush();
    }


    /**
     * 批量提交
     * @param operations
     * @throws KuduException
     */
    public void apply(List<Operation> operations) throws KuduException {
        int index = 0;
        for(Operation operation : operations){
            try {
                kuduSession.apply(operation);
                if( ++index % 8 == 0){
                    kuduSession.flush();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        kuduSession.flush();
    }

    @PreDestroy
    public void dedtroy() {
        try {
            kuduSession.close();
            kuduClient.close();
            logger.info("kudu 客户端注销完成。");
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
