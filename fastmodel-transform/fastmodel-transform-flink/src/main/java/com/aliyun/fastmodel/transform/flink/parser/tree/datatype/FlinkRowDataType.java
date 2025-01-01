package com.aliyun.fastmodel.transform.flink.parser.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.transform.flink.context.FlinkTransformContext;
import com.aliyun.fastmodel.transform.flink.parser.visitor.FlinkExpressionAstVisitor;

/**
 * @author 子梁
 * @date 2024/5/17
 */
public class FlinkRowDataType extends RowDataType {

    public FlinkRowDataType(List<Field> fields) {
        super(null, null, fields);
    }

    public FlinkRowDataType(NodeLocation location, String origin, List<Field> fields) {
        super(location, origin, fields);
    }

    @Override
    public IDataTypeName getTypeName() {
        return FlinkDataTypeName.ROW;
    }

    @Override
    public String toString() {
        return new FlinkExpressionAstVisitor(FlinkTransformContext.builder().build()).process(this);
    }
}
