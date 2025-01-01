package com.aliyun.fastmodel.transform.flink.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.flink.context.FlinkTransformContext;
import com.aliyun.fastmodel.transform.flink.format.FlinkOutVisitor;
import com.google.auto.service.AutoService;

/**
 * 默认的builder实现 支持flink的DDL输出
 *
 * @author 子梁
 * @date 2024/5/22
 */
@BuilderAnnotation(dialect = Constants.FLINK, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<FlinkTransformContext> {

    @Override
    public DialectNode build(BaseStatement source, FlinkTransformContext context) {
        FlinkOutVisitor outVisitor = new FlinkOutVisitor(context);
        Boolean process = outVisitor.process(source, 0);
        StringBuilder builder = outVisitor.getBuilder();
        return new DialectNode(builder.toString(), process);
    }

}
