package com.aliyun.fastmodel.transform.starrocks;

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
import com.aliyun.fastmodel.transform.starrocks.client.converter.StarRocksClientConverter;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksLanguageParser;
import com.google.auto.service.AutoService;

/**
 * star rocks transformer
 *
 * @author panguanjing
 * @date 2023/9/5
 */
@AutoService(Transformer.class)
@Dialect(value = Constants.STARROCKS)
public class StarRocksTransformer implements Transformer<BaseStatement> {

    private final StarRocksLanguageParser starRocksLanguageParser = new StarRocksLanguageParser();

    private final StarRocksClientConverter starRocksClientConverter = new StarRocksClientConverter();

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.STARROCKS, IVersion.getDefault());
        StarRocksContext mysqlTransformContext = new StarRocksContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, mysqlTransformContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, mysqlTransformContext);
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)starRocksLanguageParser.parseNode(dialectNode.getNode(), context);
    }

    @Override
    public Node reverseTable(Table table, ReverseContext context) {
        return starRocksClientConverter.covertToNode(table, TableConfig.builder().build());
    }

    @Override
    public Table transformTable(Node table, TransformContext context) {
        StarRocksContext starRocksContext = new StarRocksContext(context);
        return starRocksClientConverter.convertToTable(table, starRocksContext);
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        return starRocksClientConverter.getPropertyConverter().create(name, value);
    }
}
