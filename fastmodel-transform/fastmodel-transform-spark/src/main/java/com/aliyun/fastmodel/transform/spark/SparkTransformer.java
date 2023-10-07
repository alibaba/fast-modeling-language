package com.aliyun.fastmodel.transform.spark;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.aliyun.fastmodel.transform.spark.context.SparkTransformContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkLanguageParser;
import com.google.auto.service.AutoService;

/**
 * Spark Transformer
 *
 * @author panguanjing
 * @date 2023/2/13
 */
@AutoService(Transformer.class)
@Dialect(value = Constants.SPARK)
public class SparkTransformer implements Transformer<BaseStatement> {

    private SparkLanguageParser sparkLanguageParser = new SparkLanguageParser();

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.SPARK, IVersion.getDefault());
        SparkTransformContext mysqlTransformContext = new SparkTransformContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, mysqlTransformContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, mysqlTransformContext);
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)sparkLanguageParser.parseNode(dialectNode.getNode(), context);
    }
}
