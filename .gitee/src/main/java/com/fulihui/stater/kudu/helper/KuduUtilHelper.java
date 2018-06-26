package com.fulihui.stater.kudu.helper;

import com.alibaba.fastjson.util.TypeUtils;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

/**
 * @author: LiuJie
 * @version: 2018/5/7 10:43
 * @description:
 */
public class KuduUtilHelper {


    /**
     *  循环数据填充 operation 的 row
     * @param data
     * @param ktable
     * @param operation
     */
    public void fillRow(CaseInsensitiveMap<String, Object> data, KuduTable ktable, Operation operation) {
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
    public static void fillCol(PartialRow row, ColumnSchema colSchema,CaseInsensitiveMap<String, Object> data) {
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
