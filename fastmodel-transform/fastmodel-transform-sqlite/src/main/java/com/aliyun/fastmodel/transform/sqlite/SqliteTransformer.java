package com.aliyun.fastmodel.transform.sqlite;

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
import com.aliyun.fastmodel.transform.sqlite.context.SqliteContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SqliteLanguageParser;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;

/**
 * sqlite transformer
 *
 * @author panguanjing
 * @date 2023/8/14
 */
@AutoService(value = Transformer.class)
@Dialect(value = Constants.SQLITE)
public class SqliteTransformer implements Transformer<BaseStatement> {

    private final SqliteLanguageParser sqliteLanguageParser = new SqliteLanguageParser();

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.SQLITE, IVersion.getDefault());
        SqliteContext mysqlTransformContext = new SqliteContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, mysqlTransformContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, mysqlTransformContext);
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        Preconditions.checkNotNull(dialectNode.getNode());
        return (BaseStatement)sqliteLanguageParser.parseNode(dialectNode.getNode(), context);
    }
}
