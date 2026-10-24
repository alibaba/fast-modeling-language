package com.aliyun.fastmodel.transform.mc.datatype;

import java.util.Map;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.datatype.BaseDataTypeConverter;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName;
import com.google.auto.service.AutoService;
import com.google.common.collect.Maps;

/**
 * Sqlite2MaxComputeDataTypeConverter
 *
 * @author panguanjing
 * @date 2023/8/29
 */
@AutoService(DataTypeConverter.class)
public class Sqlite2MaxComputeDataTypeConverter extends BaseDataTypeConverter {
    @Override
    protected Map<String, String> getCopyMap() {
        return Maps.newHashMap();
    }

    @Override
    protected Map<String, BaseDataType> getConvertMap() {
        Map<String, BaseDataType> maps = Maps.newHashMap();
        maps.put("INTEGER", DataTypeUtil.simpleType(MaxComputeDataTypeName.INT));
        maps.put("TEXT", DataTypeUtil.simpleType(MaxComputeDataTypeName.STRING));
        maps.put("REAL", DataTypeUtil.simpleType(MaxComputeDataTypeName.DOUBLE));
        maps.put("NUMERIC", DataTypeUtil.simpleType(MaxComputeDataTypeName.DATE));
        maps.put("VARCHAR", DataTypeUtil.simpleType(MaxComputeDataTypeName.STRING));
        return maps;
    }

    @Override
    public DialectMeta getSourceDialect() {
        return DialectMeta.getByName(DialectName.SQLITE);
    }

    @Override
    public DialectMeta getTargetDialect() {
        return DialectMeta.DEFAULT_MAX_COMPUTE;
    }
}
