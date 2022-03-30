/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.mysql.datatype;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

/**
 * mysql 2 oracle dataType transformer
 * http://www.sqlines.com/mysql-to-oracle
 *
 * @author panguanjing
 * @date 2021/8/13
 */
@AutoService(DataTypeConverter.class)
public class Mysql2OracleDataTypeConverter implements DataTypeConverter {

    private static final Map<String, BaseDataType> MYSQL_2_ORACLE_DATA_TYPE_MAP = new HashMap() {
        {
            put("BIGINT", new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("19"))));
            put("BOOLEAN", DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("1")));
            put("BOOL", DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("1")));
            put("FLOAT", new GenericDataType(new Identifier("BINARY_DOUBLE")));
            put("FLOAT4", new GenericDataType(new Identifier("BINARY_DOUBLE")));
            put("FLOAT8", new GenericDataType(new Identifier("BINARY_DOUBLE")));
            put("INT", new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("10"))));
            put("INTEGER", new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("10"))));
            put("INT1", new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("3"))));
            put("INT2", new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("5"))));
            put("INT3", new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("7"))));
            put("INT4", new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("10"))));
            put("INT8", new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("19"))));
            put("LONGBLOB", new GenericDataType(new Identifier("BLOB")));
            put("LONGTEXT", new GenericDataType(new Identifier("CLOB")));
            put("LONG VARBINARY", new GenericDataType(new Identifier("BLOB")));
            put("LONG", new GenericDataType(new Identifier("CLOB")));
            put("LONG VARCHAR", new GenericDataType(new Identifier("CLOB")));
            put("MEDIUMBLOB", new GenericDataType(new Identifier("BLOB")));
            put("MEDIUMINT",
                new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("7"))));
            put("MEDIUMTEXT", new GenericDataType(new Identifier("CLOB")));
            put("MIDDLEINT",
                new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("7"))));
            put("REAL", new GenericDataType(new Identifier("BINARY_DOUBLE")));
            put("SMALLINT", new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("5"))));
            put("TEXT", new GenericDataType(new Identifier("CLOB")));
            put("TINYBLOB", new GenericDataType(new Identifier("RAW"), ImmutableList.of(new NumericParameter("255"))));
            put("TINYINT", new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("3"))));
            put("TINYTEXT",
                new GenericDataType(new Identifier("VARCHAR2"), ImmutableList.of(new NumericParameter("255"))));
        }
    };

    /**
     * 不做转换的处理
     */
    private static final Map<String, String> COPY_DATA_TYPE_MAP = new HashMap<String, String>() {
        {
            put("CHAR", "CHAR");
            put("CHARACTER", "CHARACTER");
            put("DATE", "DATE");
            put("NCHAR", "NCHAR");
            put("TIMESTAMP", "TIMESTAMP");
            put("CHARACTER VARYING", "VARCHAR2");
            put("DATETIME", "TIMESTAMP");
            put("DECIMAL", "NUMBER");
            put("DEC", "NUMBER");
            put("FIXED", "NUMBER");
            put("NVARCHAR", "NVARCHAR2");
            put("NUMERIC", "NUMBER");
            put("TIME", "TIMESTAMP");
            put("VARCHAR", "VARCHAR2");
        }
    };

    @Override
    public BaseDataType convert(BaseDataType baseDataType) {
        DataTypeEnums typeName = baseDataType.getTypeName();
        if (baseDataType instanceof RowDataType) {
            throw new UnsupportedOperationException("unsupported row dataType");
        }
        String value = null;
        if (typeName == DataTypeEnums.CUSTOM) {
            GenericDataType genericDataType = (GenericDataType)baseDataType;
            Identifier name = genericDataType.getName();
            value = name.getValue();
        } else {
            value = typeName.name();
        }
        value = value.toUpperCase(Locale.ROOT);
        BaseDataType dataType = MYSQL_2_ORACLE_DATA_TYPE_MAP.get(value);
        if (dataType != null) {
            return dataType;
        }
        GenericDataType genericDataType = (GenericDataType)baseDataType;
        String target = COPY_DATA_TYPE_MAP.get(value);
        if (StringUtils.isNotBlank(target)) {
            return DataTypeUtil.simpleType(target, genericDataType.getArguments());
        }
        //INTERVAL YEAR, INTERVAL DAY(p) TO SECOND(s)
        if (StringUtils.startsWith(value, "YEAR")) {
            return DataTypeUtil.simpleType("NUMBER", ImmutableList.of(new NumericParameter("4")));
        }
        return null;
    }

    @Override
    public DialectMeta getSourceDialect() {
        return DialectMeta.getByName(DialectName.MYSQL);
    }

    @Override
    public DialectMeta getTargetDialect() {
        return DialectMeta.getByName(DialectName.ORACLE);
    }

}