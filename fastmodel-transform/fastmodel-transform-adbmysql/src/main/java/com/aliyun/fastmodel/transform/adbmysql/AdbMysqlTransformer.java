package com.aliyun.fastmodel.transform.adbmysql;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.adbmysql.context.AdbMysqlTransformContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlLanguageParser;
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
import com.google.auto.service.AutoService;

/**
 * adb mysql transformer
 *
 * @author panguanjing
 * @date 2023/2/10
 */
@AutoService(value = Transformer.class)
@Dialect(value = Constants.ADB_MYSQL)
public class AdbMysqlTransformer implements Transformer<BaseStatement> {

    private final AdbMysqlLanguageParser adbMysqlLanguageParser = new AdbMysqlLanguageParser();

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)adbMysqlLanguageParser.parseNode(dialectNode.getNode(), context);
    }

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.ADB_MYSQL, IVersion.getDefault());
        AdbMysqlTransformContext mysqlTransformContext = new AdbMysqlTransformContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, mysqlTransformContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, mysqlTransformContext);
    }
}
