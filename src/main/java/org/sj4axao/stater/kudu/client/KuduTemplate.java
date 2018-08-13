package org.sj4axao.stater.kudu.client;

import org.apache.kudu.client.*;
import org.sj4axao.stater.kudu.config.KuduProperties;

import java.util.List;

public interface KuduTemplate {

    KuduProperties getProperties();

    long getId();

    List<String> getTablesList();

    KuduTable getTable(String tableName)  throws KuduException;

    //--------------------- return Operation 用于批量操作(apply) -------------
    Insert createInsert(String table, Object data) throws KuduException;

    Update createUpdate(String table, Object data) throws KuduException;

    Delete createDelete(String table, Object data) throws KuduException;

    Upsert createUpsert(String table, Object data) throws KuduException;

    void apply(List<Operation> operations) throws KuduException;
    void apply(Operation operation) throws KuduException;

    //---------------- 单条操作 ----------------

    void delete(String table, Object data) throws KuduException;
    void insert(String table, Object data) throws KuduException;
    void update(String table, Object data) throws KuduException;
    void upsert(String table, Object data) throws KuduException;



    // todo scan !!!
    // todo create/modify/delete table
}
