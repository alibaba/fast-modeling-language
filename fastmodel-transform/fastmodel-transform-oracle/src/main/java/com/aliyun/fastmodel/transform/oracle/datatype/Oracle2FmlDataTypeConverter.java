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
import org.apache.commons.lang3.StringUtils;

/**
 * Oracle2FmlDataTypeConverter
 * oracle 2 fml 类型转换
 *
 * @author panguanjing
 * @date 2021/8/18
 */
@AutoService(DataTypeConverter.class)
public class Oracle2FmlDataTypeConverter implements DataTypeConverter {
    private static final Map<String, BaseDataType> ORACLE_2_FML_DATA_TYPE_MAP = new HashMap() {
        {
            put("BFILE", DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, new NumericParameter("255")));
            put("BINARY_FLOAT", DataTypeUtil.simpleType(DataTypeEnums.FLOAT));
            put("BINARY_DOUBLE", DataTypeUtil.simpleType(DataTypeEnums.DOUBLE));
            put("BLOB", DataTypeUtil.simpleType(DataTypeEnums.BINARY));
            put("CLOB", DataTypeUtil.simpleType(DataTypeEnums.TEXT));
            put("DATE", DataTypeUtil.simpleType(DataTypeEnums.DATETIME));
            put("DOUBLE PRECISION", DataTypeUtil.simpleType(DataTypeEnums.DOUBLE));
            put("INTEGER", DataTypeUtil.simpleType(DataTypeEnums.INT));
            put("LONG", DataTypeUtil.simpleType(DataTypeEnums.BIGINT));
            put("LONG RAW", DataTypeUtil.simpleType(DataTypeEnums.BINARY));
            put("REAL", DataTypeUtil.simpleType(DataTypeEnums.DOUBLE));
            put("XMLTYPE", DataTypeUtil.simpleType(DataTypeEnums.TEXT));
            put("NCLOB", DataTypeUtil.simpleType(DataTypeEnums.TEXT));
            put("REAL", DataTypeUtil.simpleType(DataTypeEnums.DOUBLE));
            put("ROWID", DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("10")));
        }
    };

    /**
     * 不做转换的处理
     */
    private static final Map<String, String> COPY_DATA_TYPE_MAP = new HashMap<String, String>() {
        {
            put("SMALLINT", "SMALLINT");
            put("NUMERIC", "NUMERIC");
            put("VARCHAR", "VARCHAR");
            put("VARCHAR2", "VARCHAR");
            put("NVARCHAR2", "VARCHAR");
            put("RAW", "BINARY");
            put("TIMESTAMP", "DATETIME");
            put("UROWID", "VARCHAR");
            put("NUMBER", "DECIMAL");
            put("DECIMAL", "DECIMAL");
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
        BaseDataType dataType = ORACLE_2_FML_DATA_TYPE_MAP.get(value);
        if (dataType != null) {
            return dataType;
        }
        GenericDataType genericDataType = (GenericDataType)baseDataType;
        String target = COPY_DATA_TYPE_MAP.get(value);
        if (StringUtils.isNotBlank(target)) {
            return DataTypeUtil.simpleType(target, genericDataType.getArguments());
        }
        //INTERVAL YEAR, INTERVAL DAY(p) TO SECOND(s)
        if (StringUtils.startsWith(value, "INTERVAL")) {
            return DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, new NumericParameter("30"));
        }
        return baseDataType;
    }

    @Override
    public DialectMeta getSourceDialect() {
        return DialectMeta.getByName(DialectName.ORACLE);
    }

    @Override
    public DialectMeta getTargetDialect() {
        return DialectMeta.getByName(DialectName.FML);
    }
}
