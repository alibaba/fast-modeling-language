package com.aliyun.fastmodel.core.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;

/**
 * jsonDataType
 *
 * @author panguanjing
 * @date 2023/6/18
 */
public class JsonDataType extends RowDataType {

    public JsonDataType(NodeLocation location, String origin,
        List<Field> fields) {
        super(location, origin, fields);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitJsonDataType(this, context);
    }

    @Override
    public IDataTypeName getTypeName() {
        return DataTypeEnums.JSON;
    }
}
