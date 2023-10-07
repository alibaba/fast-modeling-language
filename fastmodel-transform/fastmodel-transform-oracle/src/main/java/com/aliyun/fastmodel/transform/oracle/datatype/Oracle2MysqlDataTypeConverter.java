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

package com.aliyun.fastmodel.transform.oracle.datatype;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
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
 * oracle 2 mysql dataType transformer
 * https://www.sqlines.com/oracle-to-mysql
 *
 * @author panguanjing
 * @date 2021/8/13
 */
@AutoService(DataTypeConverter.class)
public class Oracle2MysqlDataTypeConverter implements DataTypeConverter {

    private static final Map<String, BaseDataType> ORACLE_2_MYSQL_DATA_TYPE_MAP = new HashMap() {
        {
            put("BFILE", DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, new NumericParameter("255")));
            put("BINARY_FLOAT", DataTypeUtil.simpleType(DataTypeEnums.FLOAT));
            put("BINARY_DOUBLE", DataTypeUtil.simpleType(DataTypeEnums.DOUBLE));
            put("BLOB", new GenericDataType(new Identifier("LONGBLOB")));
            put("CLOB", new GenericDataType(new Identifier("LONGTEXT")));
            put("DATE", DataTypeUtil.simpleType(DataTypeEnums.DATETIME));
            put("DOUBLE PRECISION", new GenericDataType(new Identifier("DOUBLE PRECISION")));
            put("INTEGER", DataTypeUtil.simpleType(DataTypeEnums.INT));
            put("LONG", new GenericDataType(new Identifier("LONGTEXT")));
            put("LONG RAW", new GenericDataType(new Identifier("LONGBLOB")));
            put("REAL", DataTypeUtil.simpleType(DataTypeEnums.DOUBLE));
            put("XMLTYPE", new GenericDataType(new Identifier("LONGTEXT")));
            put("NCLOB",
                new GenericDataType(new Identifier("NVARCHAR"), ImmutableList.of(new NumericParameter("max"))));
            put("REAL", DataTypeUtil.simpleType(DataTypeEnums.DOUBLE));
        }
    };

    /**
     * 不做转换的处理
     */
    private static final Map<String, String> COPY_DATA_TYPE_MAP = new HashMap<String, String>() {
        {
            put("NCHAR VARYING", "NCHAR VARYING");
            put("NUMERIC", "NUMERIC");
            put("VARCHAR", "VARCHAR");
            put("VARCHAR2", "VARCHAR");
            put("NVARCHAR2", "NVARCHAR");
            put("RAW", "VARBINARY");
            put("TIMESTAMP", "DATETIME");
            put("UROWID", "VARCHAR");
        }
    };

    @Override
    public BaseDataType convert(BaseDataType baseDataType) {
        IDataTypeName typeName = baseDataType.getTypeName();
        if (baseDataType instanceof RowDataType) {
            throw new UnsupportedOperationException("unsupported row dataType");
        }
        String value = null;
        if (typeName == DataTypeEnums.CUSTOM) {
            GenericDataType genericDataType = (GenericDataType)baseDataType;
            value = genericDataType.getName();
        } else {
            value = typeName.getValue();
        }
        value = value.toUpperCase();
        BaseDataType dataType = ORACLE_2_MYSQL_DATA_TYPE_MAP.get(value);
        if (dataType != null) {
            return dataType;
        }
        GenericDataType genericDataType = (GenericDataType)baseDataType;
        //nchar
        String nchar = "NCHAR";
        if (StringUtils.equalsIgnoreCase(value, nchar)) {
            GenericDataType nchar1 = getNCharDataType(genericDataType, nchar);
            if (nchar1 != null) {return nchar1;}
        }
        String resultDataType = COPY_DATA_TYPE_MAP.get(value);
        if (StringUtils.isNotBlank(resultDataType)) {
            return DataTypeUtil.simpleType(resultDataType, genericDataType.getArguments());
        }
        //CHAR
        if (StringUtils.equalsIgnoreCase(value, "CHAR") ||
            StringUtils.equalsIgnoreCase(value, "CHARACTER")) {
            return getBaseDataTypeWithChar(genericDataType);
        }
        //NUMBER(p,0), NUMBER(p)
        String number = "NUMBER";
        if (StringUtils.equalsIgnoreCase(value, number)) {
            return getBaseDataTypeWithNumber(genericDataType);
        }
        //ROWID
        if (StringUtils.equalsIgnoreCase(value, "ROWID")) {
            return DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("10"));
        }
        //SMALLINT
        if (StringUtils.equalsIgnoreCase(value, "SMALLINT")) {
            return DataTypeUtil.simpleType(DataTypeEnums.DECIMAL, new NumericParameter("38"));
        }
        //TIMESTAMP(p) WITH TIME ZONE => DATETIME(p)
        if (StringUtils.startsWith(value, "TIMESTAMP") && StringUtils.endsWith(value, "ZONE")) {
            return new GenericDataType(new Identifier(DataTypeEnums.DATETIME.name()), genericDataType.getArguments());
        }
        //INTERVAL YEAR, INTERVAL DAY(p) TO SECOND(s)
        if (StringUtils.startsWith(value, "INTERVAL")) {
            return DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, new NumericParameter("30"));
        }
        return baseDataType;
    }

    private BaseDataType getBaseDataTypeWithChar(GenericDataType genericDataType) {
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        NumericParameter n = (NumericParameter)arguments.get(0);
        if (isBetween(n, 1, 256)) {
            return DataTypeUtil.simpleType(genericDataType.getName(), genericDataType.getArguments());
        }
        if (isBetween(n, 256, 2001)) {
            return DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, n);
        }
        return DataTypeUtil.simpleType(DataTypeEnums.TEXT);
    }

    private GenericDataType getNCharDataType(GenericDataType genericDataType, String nchar) {
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        NumericParameter n = (NumericParameter)arguments.get(0);
        //0~255, 返回nchar
        Integer integer = Integer.valueOf(n.getValue());
        if (integer >= 0 && integer <= 255) {
            return new GenericDataType(new Identifier(nchar), ImmutableList.of(n));
        }
        //256以上，返回nvarchar
        if (integer >= 256) {
            return new GenericDataType(new Identifier("NVARCHAR"), ImmutableList.of(n));
        }
        return null;
    }

    private BaseDataType getBaseDataTypeWithNumber(GenericDataType genericDataType) {
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        boolean argSizeIsOne = arguments.size() == 1;
        boolean argSizeIsTwoAndTwoIsZero = arguments.size() == 2 && ((NumericParameter)arguments.get(1)).getValue()
            .equals("0");
        boolean argSizeTwo = arguments.size() == 2;
        if (argSizeIsOne || argSizeIsTwoAndTwoIsZero) {
            NumericParameter n = (NumericParameter)arguments.get(0);
            //0~255, 返回nchar
            if (isBetween(n, 1, 3)) {
                return DataTypeUtil.simpleType(DataTypeEnums.TINYINT);
            }
            if (isBetween(n, 3, 5)) {
                return DataTypeUtil.simpleType(DataTypeEnums.SMALLINT);
            }
            if (isBetween(n, 5, 9)) {
                return DataTypeUtil.simpleType(DataTypeEnums.INT);
            }
            if (isBetween(n, 9, 19)) {
                return DataTypeUtil.simpleType(DataTypeEnums.BIGINT);
            }
            if (isBetween(n, 19, 38)) {
                return DataTypeUtil.simpleType(DataTypeEnums.DECIMAL, n);
            }
        } else if (argSizeTwo) {
            return new GenericDataType(new Identifier(DataTypeEnums.DECIMAL.name()), arguments);
        }
        return null;
    }

    @Override
    public DialectMeta getSourceDialect() {
        return DialectMeta.getByName(DialectName.ORACLE);
    }

    @Override
    public DialectMeta getTargetDialect() {
        return DialectMeta.getByName(DialectName.MYSQL);
    }

    private boolean isBetween(NumericParameter numericParameter, int start, int end) {
        Integer integer = Integer.valueOf(numericParameter.getValue());
        if (integer >= start && integer < end) {
            return true;
        }
        return false;
    }
}
