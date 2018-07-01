package org.sj4axao.stater.kudu.client;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kudu.client.*;

import java.util.List;

public interface BaseOperation {

    long getId();

    List<String> getTablesList();

    KuduTable getTable(String tableName)  throws KuduException;

    //--------------------- return Operation 用于批量操作(apply) -------------
    Insert createInsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException;

    Update createUpdate(String table, CaseInsensitiveMap<String, Object> data) throws KuduException;

    Delete createDelete(String table, CaseInsensitiveMap<String, Object> data) throws KuduException;

    Upsert createUpsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException;

    void apply(List<Operation> operations) throws KuduException;

    //---------------- 单条操作 ----------------
    void delete(String table, CaseInsensitiveMap<String, Object> data) throws KuduException;
    void insert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException;
    void update(String table, CaseInsensitiveMap<String, Object> data) throws KuduException;
    void upsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException;



    // todo scan !!!
}
