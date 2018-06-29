package com.sj4axao.stater.kudu.client;

import com.sj4axao.stater.kudu.exception.DefaultDBNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import org.apache.kudu.client.*;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * @author: LiuJie
 * @version: 2018/4/19 11:11
 * @description: 对外暴露的功能，适用于 impala 建的 kudu 表
 */
@Slf4j
public class KuduImpalaTemplate extends BaseTemplate {

    private static final String TABLE_PREFIX = "impala::";
    private static final String DOT = ".";
    private String defaultDataBase;

    @PostConstruct
    public void init(){
        super.init();
        defaultDataBase = Optional.ofNullable(kuduProperties.getDefaultDataBase()).orElseGet(()->{
            log.warn("注意:kudu.default-data-base 属性未配置,所有 kudu 操作都必须在方法中指定 dbName");
            return null;
        });
    }

    public KuduTable getTable(String tableName) throws KuduException {
        return getTable(getDefaultDataBase(),tableName);
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
        return scanId(getDefaultDataBase(),table,args);
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
        return createInsert(getDefaultDataBase(),table,data);
    }

    public Insert createInsert(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = getTable(dbName,table);
        Insert insert = ktable.newInsert();
        kuduUtil.fillRow(data, ktable, insert);
        return insert;
    }

    public Update createUpdate(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        return createUpdate(getDefaultDataBase(),table,data);
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
        kuduUtil.fillRow(data, ktable, update);
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
        return createDelete(getDefaultDataBase(),table,data);
    }
    public Delete createDelete(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = getTable(dbName,table);
        Delete delete = ktable.newDelete();
        kuduUtil.fillRow(data, ktable, delete);
        return delete;
    }

    /**
     * @param table
     * @param data 必须包含 主键 和 not null 字段
     * @return
     * @throws KuduException
     */
    public Upsert createUpsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        return createUpsert(getDefaultDataBase(),table,data);
    }
    public Upsert createUpsert(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = getTable(dbName,table);
        Upsert upsert = ktable.newUpsert();
        kuduUtil.fillRow(data, ktable, upsert);
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
        delete(getDefaultDataBase(),table,data);
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
        insert(getDefaultDataBase(),table,data);
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
        update(getDefaultDataBase(),table,data);
    }
    public void update(String dbName,String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        Update update = createUpdate(dbName,table, data);
        kuduSession.apply(update);
        kuduSession.flush();
    }

    public void upsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        upsert(getDefaultDataBase(),table,data);
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

    public String getDefaultDataBase() {
        return Optional.ofNullable(defaultDataBase).orElseThrow(() -> new DefaultDBNotFoundException("kudu.default-data-base 属性未配置"));
    }
}
