package com.aliyun.fastmodel.transform.sqlite.datatype;

import java.util.Map;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.datatype.BaseDataTypeConverter;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.aliyun.fastmodel.transform.sqlite.parser.datatype.SqliteDataTypeName;
import com.google.auto.service.AutoService;
import com.google.common.collect.Maps;

/**
 * 用于数据类型转换处理
 * https://www.sqlite.org/draft/datatype3.html
 *
 * @author panguanjing
 * @date 2023/8/14
 */
@AutoService(DataTypeConverter.class)
public class Fml2SqliteDataTypeConverter extends BaseDataTypeConverter {

    @Override
    protected Map<String, String> getCopyMap() {
        return Maps.newHashMap();
    }

    @Override
    protected Map<String, BaseDataType> getConvertMap() {
        Map<String, BaseDataType> map = Maps.newHashMap();
        //int
        map.put(DataTypeEnums.INT.getValue(), DataTypeUtil.simpleType(SqliteDataTypeName.INTEGER));
        map.put("INTEGER", DataTypeUtil.simpleType(SqliteDataTypeName.INTEGER));
        map.put("SMALLINT", DataTypeUtil.simpleType(SqliteDataTypeName.INTEGER));
        map.put("MEDIUMINT", DataTypeUtil.simpleType(SqliteDataTypeName.INTEGER));
        map.put("BIGINT", DataTypeUtil.simpleType(SqliteDataTypeName.INTEGER));

        //string
        map.put("CHAR", DataTypeUtil.simpleType(SqliteDataTypeName.TEXT));
        map.put("VARCHAR", DataTypeUtil.simpleType(SqliteDataTypeName.TEXT));
        map.put("STRING", DataTypeUtil.simpleType(SqliteDataTypeName.TEXT));
        map.put("JSON", DataTypeUtil.simpleType(SqliteDataTypeName.TEXT));

        //real
        map.put(DataTypeEnums.DOUBLE.getValue(), DataTypeUtil.simpleType(SqliteDataTypeName.REAL));
        map.put(DataTypeEnums.FLOAT.getValue(), DataTypeUtil.simpleType(SqliteDataTypeName.REAL));

        //date
        map.put(DataTypeEnums.DATE.getValue(), DataTypeUtil.simpleType(SqliteDataTypeName.NUMERIC));
        map.put(DataTypeEnums.DATETIME.getValue(), DataTypeUtil.simpleType(SqliteDataTypeName.NUMERIC));
        map.put(DataTypeEnums.TIMESTAMP.getValue(), DataTypeUtil.simpleType(SqliteDataTypeName.NUMERIC));
        map.put(DataTypeEnums.BOOLEAN.getValue(), DataTypeUtil.simpleType(SqliteDataTypeName.NUMERIC));
        return map;
    }

    @Override
    public DialectMeta getSourceDialect() {
        return new DialectMeta(DialectName.FML, IVersion.DEFAULT_VERSION);
    }

    @Override
    public DialectMeta getTargetDialect() {
        return new DialectMeta(DialectName.SQLITE, IVersion.DEFAULT_VERSION);
    }
}
