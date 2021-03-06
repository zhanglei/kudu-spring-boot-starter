package org.sj4axao.stater.kudu.client.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.client.*;
import org.sj4axao.stater.kudu.client.KuduImpalaTemplate;
import org.sj4axao.stater.kudu.client.KuduTemplate;
import org.sj4axao.stater.kudu.config.KuduProperties;
import org.sj4axao.stater.kudu.exception.DefaultDBNotFoundException;

import java.util.List;
import java.util.Optional;

/**
 * @author: LiuJie
 * @version: 2018/4/19 11:11
 * @description: 适用于 impala 建的 kudu 表
 */
@Slf4j
public class PlainKuduImpalaTemplate implements KuduImpalaTemplate {

    private static final String TABLE_PREFIX = "impala::";
    private static final String DOT = ".";
    private String defaultDataBase;
    private KuduProperties kuduProperties;
    private KuduTemplate kuduTemplate;

    public PlainKuduImpalaTemplate(KuduTemplate kuduTemplate){
        this.kuduTemplate = kuduTemplate;
        this.kuduProperties = kuduTemplate.getProperties();

        defaultDataBase = Optional.ofNullable(kuduProperties.getDefaultDataBase()).orElseGet(()->{
            log.warn("注意:kudu.default-data-base 属性未配置,所有 kudu 操作都必须在方法中指定 dbName");
            return null;
        });
    }

    @Override
    public long getId() {
        return kuduTemplate.getId();
    }

    @Override
    public List<String> getTablesList() {
        return kuduTemplate.getTablesList();
    }

    @Override
    public KuduTable getTable(String tableName) throws KuduException {
        return this.getTable(getDefaultDataBase(),tableName);
    }
    @Override
    public KuduTable getTable(String dbName ,String tableName) throws KuduException {
        String finalTableName = getFinalTableName(dbName, tableName);
        return kuduTemplate.getTable(finalTableName);
    }


    // todo scanId 不具有通用性，改为 scan 传 字段 List 或对象,现在先不动。
//    public Long scanId(String table,Map<String,String> args) throws KuduException {
//        return scanId(getDefaultDataBase(),table,args);
//    }
//    public Long scanId(String dbName,String table,Map<String,String> args) throws KuduException {
//
//        if(CollectionUtils.isEmpty(args)){
//            return null;
//        }
//
//        KuduTable ktable = getTable(dbName,table);
//        KuduScanner.KuduScannerBuilder scanner = kuduClient.newScannerBuilder(ktable);
//
//        //指定kudu返回字段：默认只返回id
//        List<String> projectColumns = new ArrayList<>(1);
//        projectColumns.add("id");
//
//        //组装条件
//        args.forEach((k,v) -> {
//            KuduPredicate predicate = KuduPredicate.newComparisonPredicate(ktable.getSchema().getColumn(k.toLowerCase()),
//                    KuduPredicate.ComparisonOp.EQUAL, v);
//            scanner.addPredicate(predicate);
//        });
//
//        KuduScanner build = scanner.build();
//        while (build.hasMoreRows()) {
//            RowResultIterator results = build.nextRows();
//            while (results.hasNext()) {
//                RowResult result = results.next();
//                return result.getLong(0);
//            }
//        }
//        return null;
//    }

    /* ***************************** 构建operation对象 *******************************************************************/

    @Override
    public Insert createInsert(String table, Object data) throws KuduException {
        return this.createInsert(getDefaultDataBase(),table,data);
    }
    @Override
    public Insert createInsert(String dbName,String table, Object data) throws KuduException {

        return kuduTemplate.createInsert(getFinalTableName(dbName,table),data);
    }

    @Override
    public Update createUpdate(String table, Object data) throws KuduException {
        return this.createUpdate(getDefaultDataBase(),table,data);
    }

    /**
     * 构建并返回一个删除操作，一般用于批量 apply
     * @param dbName
     * @param table
     * @param data 一定要有主键字段
     * @return
     * @throws KuduException
     */
    @Override
    public Update createUpdate(String dbName,String table, Object data) throws KuduException {
        return kuduTemplate.createUpdate(getFinalTableName(dbName,table),data);
    }

    /**
     * 构建删除
     * @param table String tabelName
     * @param data <column,value>删除条件 必须要有主键，存在其他条件的时候 and 关系，不符合就不删除
     * @return
     * @throws KuduException
     */
    @Override
    public Delete createDelete(String table, Object data) throws KuduException {
        return this.createDelete(getDefaultDataBase(),table,data);
    }
    @Override
    public Delete createDelete(String dbName,String table, Object data) throws KuduException {
        return kuduTemplate.createDelete(getFinalTableName(dbName,table),data);
    }

    /**
     * @param table
     * @param data 必须包含 主键 和 not null 字段
     * @return
     * @throws KuduException
     */
    @Override
    public Upsert createUpsert(String table, Object data) throws KuduException {
        return this.createUpsert(getDefaultDataBase(),table,data);
    }

    @Override
    public Upsert createUpsert(String dbName,String table, Object data) throws KuduException {
        return kuduTemplate.createUpsert(this.getFinalTableName(dbName,table),data);
    }


    @Override
    public void apply(List<Operation> operations) throws KuduException {
        kuduTemplate.apply(operations);
    }

    @Override
    public void apply(Operation operation) throws KuduException {
        kuduTemplate.apply(operation);
    }
    /* *********************** 单条操作 **********************************************************************************/

    /**
     * 单条删除
     * @param table
     * @param data
     * @throws KuduException
     */
    @Override
    public void delete(String table, Object data) throws KuduException {
        this.delete(getDefaultDataBase(),table,data);
    }
    @Override
    public void delete(String dbName,String table, Object data) throws KuduException {
        kuduTemplate.delete(getFinalTableName(dbName,table),data);
    }

    /**
     * 单条插入
     * @param table
     * @param data
     * @throws KuduException
     */
    @Override
    public void insert(String table, Object data) throws KuduException {
        this.insert(getDefaultDataBase(),table,data);
    }

    @Override
    public void insert(String dbName,String table, Object data) throws KuduException {
        kuduTemplate.insert(getFinalTableName(dbName,table),data);
    }

    /**
     * 单条更新
     * @param table
     * @param data
     * @throws KuduException
     */
    @Override
    public void update(String table, Object data) throws KuduException {
        this.update(getDefaultDataBase(),table,data);
    }
    @Override
    public void update(String dbName,String table, Object data) throws KuduException {
        kuduTemplate.update(getFinalTableName(dbName,table),data);
    }

    @Override
    public void upsert(String table, Object data) throws KuduException {
        this.upsert(getDefaultDataBase(),table,data);
    }
    @Override
    public void upsert(String dbName,String table, Object data) throws KuduException {
        kuduTemplate.upsert(getFinalTableName(dbName,table),data);
    }



    @Override
    public String getDefaultDataBase() {
        return Optional.ofNullable(defaultDataBase)
                .orElseThrow(() -> new DefaultDBNotFoundException(
                        "kudu.default-data-base 属性未配置,KuduImpalaTemplate用于操作Impala创建管理的kudu表，需要设置impala的默认数据库"));
    }

    private String getFinalTableName(String dbName, String tableName) {
        return TABLE_PREFIX + dbName + DOT + tableName;
    }
}
