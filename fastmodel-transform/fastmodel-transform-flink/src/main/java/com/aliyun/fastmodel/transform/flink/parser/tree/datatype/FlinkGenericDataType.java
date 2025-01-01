package com.aliyun.fastmodel.transform.flink.parser.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.flink.context.FlinkTransformContext;
import com.aliyun.fastmodel.transform.flink.parser.visitor.FlinkAstVisitor;
import com.aliyun.fastmodel.transform.flink.parser.visitor.FlinkExpressionAstVisitor;

/**
 * @author 子梁
 * @date 2024/5/17
 */
public class FlinkGenericDataType extends GenericDataType {

    public FlinkGenericDataType(String dataTypeName) {
        super(dataTypeName);
    }

    public FlinkGenericDataType(String dataTypeName, List<DataTypeParameter> arguments) {
        super(dataTypeName, arguments);
    }

    public FlinkGenericDataType(NodeLocation location, String origin, String dataTypeName, List<DataTypeParameter> arguments) {
        super(location, origin, dataTypeName, arguments);
    }

    @Override
    public IDataTypeName getTypeName() {
        return FlinkDataTypeName.getByValue(getName());
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        FlinkAstVisitor<R, C> flinkAstVisitor = (FlinkAstVisitor<R, C>)visitor;
        return flinkAstVisitor.visitFlinkGenericDataType(this, context);
    }

    @Override
    public String toString() {
        return new FlinkExpressionAstVisitor(FlinkTransformContext.builder().build()).process(this);
    }
}
