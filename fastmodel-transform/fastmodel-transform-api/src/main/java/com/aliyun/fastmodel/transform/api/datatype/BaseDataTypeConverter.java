package com.aliyun.fastmodel.transform.api.datatype;

import java.util.Locale;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * base data type converter
 *
 * @author panguanjing
 * @date 2023/8/14
 */
public abstract class BaseDataTypeConverter implements DataTypeConverter {

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
        value = value.toUpperCase(Locale.ROOT);
        Map<String, BaseDataType> convertMap = getConvertMap();
        BaseDataType dataType = convertMap.get(value);
        if (dataType != null) {
            return dataType;
        }
        GenericDataType genericDataType = (GenericDataType)baseDataType;
        String target = getCopyMap().get(value);
        if (StringUtils.isNotBlank(target)) {
            return DataTypeUtil.simpleType(target, genericDataType.getArguments());
        }
        return baseDataType;
    }

    /**
     * 获取copy的map
     *
     * @return
     */
    protected abstract Map<String, String> getCopyMap();

    /**
     * 获取converter map
     *
     * @return
     */
    protected abstract Map<String, BaseDataType> getConvertMap();
}
