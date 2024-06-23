package com.aliyun.fastmodel.transform.doris;

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
import com.aliyun.fastmodel.transform.doris.client.DorisClientConverter;
import com.aliyun.fastmodel.transform.doris.context.DorisContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisLanguageParser;
import com.google.auto.service.AutoService;

/**
 * doris transformer
 *
 * @author panguanjing
 * @date 2024/1/20
 */
@AutoService(Transformer.class)
@Dialect(value = Constants.DORIS)
public class DorisTransformer implements Transformer<BaseStatement> {

    private final DorisLanguageParser dorisLanguageParser = new DorisLanguageParser();

    private final DorisClientConverter dorisClientConverter = new DorisClientConverter();

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)dorisLanguageParser.parseNode(dialectNode.getNode(), context);
    }

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.DORIS, IVersion.getDefault());
        DorisContext starRocksContext = new DorisContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, starRocksContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, starRocksContext);
    }

    @Override
    public Node reverseTable(Table table, ReverseContext context) {
        return dorisClientConverter.covertToNode(table, TableConfig.builder().build());
    }

    @Override
    public Table transformTable(Node table, TransformContext context) {
        return dorisClientConverter.convertToTable(table, new DorisContext(context));
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        return dorisClientConverter.getPropertyConverter().create(name, value);
    }
}
