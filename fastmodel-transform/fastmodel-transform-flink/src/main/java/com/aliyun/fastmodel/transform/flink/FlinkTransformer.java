package com.aliyun.fastmodel.transform.flink;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.aliyun.fastmodel.transform.flink.client.converter.FlinkClientConverter;
import com.aliyun.fastmodel.transform.flink.context.FlinkTransformContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkLanguageParser;
import com.google.auto.service.AutoService;

/**
 * Flink Transformer
 * 
 * @author 子梁
 * @date 2024/5/14
 */
@AutoService(Transformer.class)
@Dialect(value = Constants.FLINK)
public class FlinkTransformer implements Transformer<BaseStatement> {

    private final FlinkLanguageParser flinkLanguageParser = new FlinkLanguageParser();

    private final FlinkClientConverter flinkClientConverter = new FlinkClientConverter();

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement) flinkLanguageParser.parseNode(dialectNode.getNode(), context);
    }

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.FLINK, IVersion.getDefault());
        FlinkTransformContext flinkTransformContext = new FlinkTransformContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, flinkTransformContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, flinkTransformContext);
    }

    @Override
    public Node reverseTable(Table table, ReverseContext context) {
        return flinkClientConverter.convertToNode(table, TableConfig.builder().build());
    }

    @Override
    public Table transformTable(Node table, TransformContext context) {
        return flinkClientConverter.convertToTable(table, new FlinkTransformContext(context));
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        return flinkClientConverter.getPropertyConverter().create(name, value);
    }

}
