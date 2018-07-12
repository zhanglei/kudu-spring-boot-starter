package org.sj4axao.stater.kudu.client;

import org.apache.kudu.client.*;
import java.util.List;

/**
 * @author: LiuJie
 * @version: 2018/7/12 9:50
 * @description: 对外暴露的功能，适用于 impala 建的 kudu 表
 */
public interface KuduImpalaTemplate {

    long getId();
    List<String> getTablesList() ;

    KuduTable getTable(String tableName) throws KuduException ;
    KuduTable getTable(String dbName ,String tableName) throws KuduException ;

    /* ***************************** 构建operation对象 *******************************************************************/
    Insert createInsert(String table, Object data) throws KuduException ;
    Insert createInsert(String dbName,String table, Object data) throws KuduException ;

    /**
     * 构建并返回一个删除操作，一般用于批量 apply
     * @param table
     * @param data 一定要有主键字段
     * @return
     * @throws KuduException
     */
    Update createUpdate(String table, Object data) throws KuduException ;
    Update createUpdate(String dbName,String table, Object data) throws KuduException ;

    /**
     * 构建删除
     * @param table String tabelName
     * @param data <column,value>删除条件 必须要有主键，存在其他条件的时候 and 关系，不符合就不删除
     * @return
     * @throws KuduException
     */
    Delete createDelete(String table, Object data) throws KuduException ;
    Delete createDelete(String dbName,String table, Object data) throws KuduException ;

    /**
     * @param table
     * @param data 必须包含 主键 和 not null 字段
     * @return
     * @throws KuduException
     */
    Upsert createUpsert(String table, Object data) throws KuduException ;
    Upsert createUpsert(String dbName,String table, Object data) throws KuduException ;

    void apply(List<Operation> operations) throws KuduException ;
    void apply(Operation operation) throws KuduException ;
    /* *********************** 单条操作 **********************************************************************************/

    /**
     * 单条删除
     * @param table
     * @param data
     * @throws KuduException
     */
    void delete(String table, Object data) throws KuduException ;
    void delete(String dbName,String table, Object data) throws KuduException ;

    /**
     * 单条插入
     * @param table
     * @param data
     * @throws KuduException
     */
    void insert(String table, Object data) throws KuduException ;
    void insert(String dbName,String table, Object data) throws KuduException ;

    /**
     * 单条更新
     * @param table
     * @param data
     * @throws KuduException
     */
    void update(String table, Object data) throws KuduException ;
    void update(String dbName,String table, Object data) throws KuduException ;

    void upsert(String table, Object data) throws KuduException ;
    void upsert(String dbName,String table, Object data) throws KuduException ;

    String getDefaultDataBase() ;
}
