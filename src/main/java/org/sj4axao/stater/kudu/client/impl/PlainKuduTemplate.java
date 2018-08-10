package org.sj4axao.stater.kudu.client.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.util.TypeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.sj4axao.stater.kudu.client.KuduTemplate;
import org.sj4axao.stater.kudu.config.KuduProperties;
import org.sj4axao.stater.kudu.utils.IdGenerator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: LiuJie
 * @version: 2018/6/29 16:56
 * @description:
 */
@Slf4j
public class PlainKuduTemplate implements KuduTemplate {
    private KuduSession kuduSession;
    private KuduClient kuduClient;
    KuduProperties kuduProperties;

    private IdGenerator idGenerator;
    private static Map<String, KuduTable> tables ;

    public PlainKuduTemplate(KuduClient kuduClient,KuduSession kuduSession,KuduProperties kuduProperties){
        this.kuduClient = kuduClient;
        this.kuduSession = kuduSession;
        this.kuduProperties = kuduProperties;

        // 初始化 id 生成器
        tables = new HashMap<>();
        Long wordId = kuduProperties.getWorkerId();
        log.info("默认workId ={},本节点的 workId = {}",KuduProperties.DEFAULT_WORK_ID,wordId);
        idGenerator = new IdGenerator(wordId);
    }

    @Override
    public KuduProperties getProperties() {
        return kuduProperties;
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
    @Override
    public void apply(List<Operation> operations) throws KuduException {
        long start = System.currentTimeMillis();
        int index = 0;
        for(Operation operation : operations){
            try {
                kuduSession.apply(operation);
                if( ++index % 8 == 0){
                    printResposse(kuduSession.flush());
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        printResposse(kuduSession.flush());
        log.info("kudu 请求条数={}条，处理时间={} ms",operations.size(),System.currentTimeMillis()-start);
    }

    /**
     * 单条提交
     * @param operation
     * @throws KuduException
     */
    @Override
    public void apply(Operation operation) throws KuduException {
        this.apply(Collections.singletonList(operation));
    }

    /**
     * 打印失败的结果
     * @param responses
     */
    private void printResposse(List<OperationResponse> responses){
        if(responses == null || responses.isEmpty()){
            return;
        }
        List<RowError> rowErrors = OperationResponse.collectErrors(responses);
        if(!rowErrors.isEmpty()){
            log.error("kudu {}条操作请求失败！tips={}",rowErrors.size(),rowErrors);
        }
    }
// -----------------------------------------------------

    @Override
    public KuduTable getTable(String tableName) throws KuduException {

        KuduTable table = tables.get(tableName);
        if (table == null) {
            table = kuduClient.openTable(tableName);
            tables.put(tableName, table);
        }
        return table;
    }
    @Override
    public Insert createInsert(String table, Object data) throws KuduException {
        KuduTable ktable = this.getTable(table);
        Insert insert = ktable.newInsert();
        this.fillData(data, ktable, insert);
        return insert;
    }

    /**
     * 构建并返回一个删除操作，一般用于批量 apply
     * @param table
     * @param data 一定要有主键字段
     * @return
     * @throws KuduException
     */
    @Override
    public Update createUpdate(String table, Object data) throws KuduException {
        KuduTable ktable = this.getTable(table);
        Update update = ktable.newUpdate();
        this.fillData(data, ktable, update);
        return update;
    }
    @Override
    /**
     * 删除
     */
    public Delete createDelete(String table, Object data) throws KuduException {
        KuduTable ktable = this.getTable(table);
        Delete delete = ktable.newDelete();
        this.fillData(data, ktable, delete);
        return delete;
    }
    @Override
    public Upsert createUpsert(String table, Object data) throws KuduException {
        KuduTable ktable = this.getTable(table);
        Upsert upsert = ktable.newUpsert();
        this.fillData(data, ktable, upsert);
        return upsert;
    }
    @Override
    public void delete(String table, Object data) throws KuduException {
        Delete delete = this.createDelete(table, data);
        this.apply(delete);
    }
    @Override
    public void insert(String table, Object data) throws KuduException {
        Insert insert = this.createInsert(table, data);
        this.apply(insert);
    }
    @Override
    public void update(String table, Object data) throws KuduException {
        Update update = this.createUpdate(table, data);
        this.apply(update);
    }
    @Override
    public void upsert(String table, Object data) throws KuduException {
        Upsert upsert = this.createUpsert(table, data);
        this.apply(upsert);
    }
    //---------------------------------------------------------

    /**
     * 为数据赋值到 operation 中
     * @param data
     * @param ktable
     * @param operation
     * @param data
     */
    private void  fillData(Object data,KuduTable ktable,Operation operation){
        JSON.toJSONString(data);
        PartialRow row = operation.getRow();
        Schema schema = ktable.getSchema();
        Map<String, ColumnSchema> cols = getCols(schema);

        Class<?> clazz = data.getClass();
        for (Method method : clazz.getMethods()) {
            String methodName = method.getName();
            // 匹配获取无参get方法
            if(methodName.startsWith("get") && method.getParameterCount() == 0 && !"getClass".equals(methodName)){
                //去掉 _ 且转为小写,从 cols 里面匹配字段
                ColumnSchema columnSchema = cols.get(transColName(methodName.substring(3)));
                if(null == columnSchema){
                    // 没有该字段，跳过
                    continue;
                }
                Object value = null;
                try {
                    value = method.invoke(data);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                if(null == value){
                    // 字段值为 null ,跳过
                    // 不过这样会导致无法设置某个字段值为 null
                    continue;
                }
                // 赋值
                fillCol(row,columnSchema,value);
            }
        }

    }

    /**
     * get table 所有的字段
     * @param schema
     * @return
     */
    private Map<String, ColumnSchema> getCols(Schema schema){
        Map<String, ColumnSchema> data = new HashMap<>();
        for (ColumnSchema columnSchema : schema.getColumns()) {
            data.put(transColName(columnSchema.getName()),columnSchema);
        }
        return data;
    }

    /**
     * 匹配时去掉 _ 且 不区分大小写
     * @param colName
     * @return
     */
    private String transColName(String colName){
        return colName.replaceAll("_", "").toLowerCase();
    }

    /**
     * 判断每个字段的类型，填充数据
     * @param row
     * @param colSchema
     * @param value
     */
    private static void fillCol(PartialRow row, ColumnSchema colSchema,Object value) {
        String name = colSchema.getName();
        if (null == value) {
            return;
        }
        Type type = colSchema.getType();
        switch (type) {
            case STRING:
                row.addString(name, TypeUtils.castToString(value));
                break;
            case INT64:
            case UNIXTIME_MICROS:
                row.addLong(name, TypeUtils.castToLong(value));
                break;
            case DOUBLE:
                row.addDouble(name, TypeUtils.castToDouble(value));
                break;
            case INT32:
                row.addInt(name, TypeUtils.castToInt(value));
                break;
            case INT16:
                row.addShort(name, TypeUtils.castToShort(value));
                break;
            case INT8:
                row.addByte(name,TypeUtils.castToByte(value));
                break;
            case BOOL:
                row.addBoolean(name, TypeUtils.castToBoolean(value));
                break;
            case BINARY:
                row.addBinary(name, TypeUtils.castToBytes(value));
                break;
            case FLOAT:
                row.addFloat(name, TypeUtils.castToFloat(value));
                break;
            default:
                break;
        }
    }



}
