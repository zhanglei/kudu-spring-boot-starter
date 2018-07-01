package org.sj4axao.stater.kudu.client;

import com.alibaba.fastjson.util.TypeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.sj4axao.stater.kudu.config.KuduProperties;
import org.sj4axao.stater.kudu.utils.IdGenerator;
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
public abstract class BaseTemplate implements BaseOperation {
    @Autowired
    protected KuduSession kuduSession;
    @Autowired
    protected KuduClient kuduClient;
    @Autowired
    protected KuduProperties kuduProperties;

    protected IdGenerator idGenerator;
    protected static Map<String, KuduTable> tables ;

    protected void init(){
        tables = new HashMap<>();
        // 初始化 id 生成器
        Long wordId = kuduProperties.getWorkerId();
        log.info("默认workId ={},本节点的 workId = {}",KuduProperties.DEFAULT_WORK_ID,wordId);
        idGenerator = new IdGenerator(wordId);
    }

    /**
     * @return 不重复的 Long 类型 id
     */
    @Override
    public long getId(){
        return idGenerator.nextId();
    }


    /**
     * 获取table列表
     * 数据库是Impala的定义，kudu没有的，只是表名不同，
     * 所以
     * @return 所有库的所有表，即 kudu 中的所有表
     */
    @Override
    public List<String> getTablesList(){
        try {
            return kuduClient.getTablesList().getTablesList();
        } catch (KuduException e) {
            e.printStackTrace();
            return null;
        }
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

    /**
     * 单条提交
     * @param operation
     * @throws KuduException
     */
    public void apply(Operation operation) throws KuduException {
        kuduSession.apply(operation);
        kuduSession.flush();
    }
// -----------------------------------------------------

    public KuduTable getTable(String tableName) throws KuduException {

        KuduTable table = tables.get(tableName);
        if (table == null) {
            table = kuduClient.openTable(tableName);
            tables.put(tableName, table);
        }
        return table;
    }

    public Insert createInsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = this.getTable(table);
        Insert insert = ktable.newInsert();
        this.fillRow(data, ktable, insert);
        return insert;
    }

    /**
     * 构建并返回一个删除操作，一般用于批量 apply
     * @param table
     * @param data 一定要有主键字段
     * @return
     * @throws KuduException
     */
    public Update createUpdate(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = this.getTable(table);
        Update update = ktable.newUpdate();
        this.fillRow(data, ktable, update);
        return update;
    }

    public Delete createDelete(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = this.getTable(table);
        Delete delete = ktable.newDelete();
        this.fillRow(data, ktable, delete);
        return delete;
    }

    public Upsert createUpsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        KuduTable ktable = this.getTable(table);
        Upsert upsert = ktable.newUpsert();
        this.fillRow(data, ktable, upsert);
        return upsert;
    }

    public void delete(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        Delete delete = this.createDelete(table, data);
        this.apply(delete);
    }

    public void insert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        Insert insert = this.createInsert(table, data);
        this.apply(insert);
    }
    public void update(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        Update update = this.createUpdate(table, data);
        this.apply(update);
    }
    public void upsert(String table, CaseInsensitiveMap<String, Object> data) throws KuduException {
        Upsert upsert = this.createUpsert(table, data);
        this.apply(upsert);
    }
    //---------------------------------------------------------

    /**
     *  循环数据填充 operation 的 row
     * @param data
     * @param ktable
     * @param operation
     */
    protected void fillRow(CaseInsensitiveMap<String, Object> data, KuduTable ktable, Operation operation) {
        PartialRow row = operation.getRow();
        Schema schema = ktable.getSchema();
        for (String colName : data.keySet()) {
            ColumnSchema colSchema = schema.getColumn(colName);
            fillCol(row, colSchema, data);
        }
    }

    /**
     * 判断每个字段的类型，填充数据
     * @param row
     * @param colSchema
     * @param data
     */
    protected static void fillCol(PartialRow row, ColumnSchema colSchema,CaseInsensitiveMap<String, Object> data) {
        String name = colSchema.getName();
        if (data.get(name) == null) {
            return;
        }
        Type type = colSchema.getType();
        Object object = data.get(name);
        switch (type) {
            case STRING:
                row.addString(name, TypeUtils.castToString(object));
                break;
            case INT64:
            case UNIXTIME_MICROS:
                row.addLong(name, TypeUtils.castToLong(object));
                break;
            case DOUBLE:
                row.addDouble(name, TypeUtils.castToDouble(object));
                break;
            case INT32:
                row.addInt(name, TypeUtils.castToInt(object));
                break;
            case INT16:
                row.addShort(name, TypeUtils.castToShort(object));
                break;
            case INT8:
                row.addByte(name,TypeUtils.castToByte(object));
                break;
            case BOOL:
                row.addBoolean(name, TypeUtils.castToBoolean(object));
                break;
            case BINARY:
                row.addBinary(name, TypeUtils.castToBytes(object));
                break;
            case FLOAT:
                row.addFloat(name, TypeUtils.castToFloat(object));
                break;
            default:
                break;
        }
    }
}
