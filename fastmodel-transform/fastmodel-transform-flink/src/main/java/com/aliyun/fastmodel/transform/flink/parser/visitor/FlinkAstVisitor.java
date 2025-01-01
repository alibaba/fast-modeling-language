package com.aliyun.fastmodel.transform.flink.parser.visitor;

import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkGenericDataType;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkRawDataType;

/**
 * @author 子梁
 * @date 2024/5/17
 */
public interface FlinkAstVisitor<R, C> extends ExtensionAstVisitor<R, C> {

    /**
     * visit flink generic DataType
     *
     * @param flinkGenericDataType
     * @param context
     * @return
     */
    default R visitFlinkGenericDataType(FlinkGenericDataType flinkGenericDataType, C context) {
        return visitGenericDataType(flinkGenericDataType, context);
    }

    /**
     * visit flink Raw DataType
     *
     * @param flinkRawDataType
     * @param context
     * @return
     */
    default R visitFlinkRawDataType(FlinkRawDataType flinkRawDataType, C context) {
        return visitDataType(flinkRawDataType, context);
    }

}
