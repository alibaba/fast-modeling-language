package com.aliyun.fastmodel.transform.oceanbase;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
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
import com.aliyun.fastmodel.transform.oceanbase.client.converter.OceanBaseMysqlClientConverter;
import com.aliyun.fastmodel.transform.oceanbase.context.OceanBaseContext;
import com.aliyun.fastmodel.transform.oceanbase.parser.OceanBaseMysqlLanguageParser;
import com.google.auto.service.AutoService;

/**
 * OceanBaseTransformer
 *
 * @author panguanjing
 * @date 2024/2/2
 */
@Dialect(value = Constants.OB_MYSQL)
@AutoService(Transformer.class)
public class OceanBaseMysqlTransformer implements Transformer<BaseStatement> {

    private final OceanBaseMysqlLanguageParser oceanBaseMysqlLanguageParser = new OceanBaseMysqlLanguageParser();

    private final OceanBaseMysqlClientConverter oceanBaseMysqlClientConverter = new OceanBaseMysqlClientConverter();

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.OB_MYSQL, IVersion.getDefault());
        OceanBaseContext oceanBaseContext = new OceanBaseContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, oceanBaseContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, oceanBaseContext);
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return oceanBaseMysqlLanguageParser.parseNode(dialectNode.getNode(), context);
    }

    @Override
    public Node reverseTable(Table table, ReverseContext context) {
        return oceanBaseMysqlClientConverter.convertToNode(table, TableConfig.builder().build());
    }

    @Override
    public Table transformTable(Node table, TransformContext context) {
        return oceanBaseMysqlClientConverter.convertToTable(table, new OceanBaseContext(context));
    }
}
