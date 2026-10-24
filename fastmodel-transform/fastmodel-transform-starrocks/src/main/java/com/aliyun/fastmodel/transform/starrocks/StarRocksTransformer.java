package com.aliyun.fastmodel.transform.starrocks;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
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
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksOutVisitor;
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
    public List<DialectNode> transformStatements(List<? extends BaseStatement> statements, Node target,
        TransformContext context) {
        List<DialectNode> result = new ArrayList<>();
        for (int i = 0; i < statements.size();) {
            BaseStatement statement = statements.get(i);
            if (statement instanceof CompositeStatement) {
                result.addAll(transformStatements(((CompositeStatement)statement).getStatements(), target, context));
                i++;
                continue;
            }
            if (!(target instanceof CreateTable) || !StarRocksOutVisitor.isSchemaChange(statement)) {
                result.add(transform(statement, new TransformContext(context)));
                i++;
                continue;
            }
            List<BaseStatement> changes = new ArrayList<>();
            BaseOperatorStatement first = (BaseOperatorStatement)statement;
            while (i < statements.size() && StarRocksOutVisitor.isSchemaChange(statements.get(i))
                && first.getQualifiedName().equals(((BaseOperatorStatement)statements.get(i)).getQualifiedName())) {
                changes.add(statements.get(i++));
            }
            // A positioned MODIFY already includes the complete new definition of the column.
            Set<Identifier> movedColumns = changes.stream().filter(SetColumnOrder.class::isInstance)
                .map(SetColumnOrder.class::cast).map(SetColumnOrder::getNewColName).collect(Collectors.toSet());
            changes.removeIf(change -> change instanceof ChangeCol
                && movedColumns.contains(((ChangeCol)change).getOldColName()));
            StarRocksOutVisitor visitor = new StarRocksOutVisitor(new StarRocksContext(context), (CreateTable)target);
            BaseStatement grouped = changes.size() == 1 ? changes.get(0) : new CompositeStatement(changes);
            Boolean executable = visitor.process(grouped, 0);
            result.add(new DialectNode(visitor.getBuilder().toString(), executable));
        }
        return result;
    }

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.STARROCKS, IVersion.getDefault());
        StarRocksContext starRocksContext = new StarRocksContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, starRocksContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, starRocksContext);
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)starRocksLanguageParser.parseNode(dialectNode.getNode(), context);
    }

    @Override
    public Node reverseTable(Table table, ReverseContext context) {
        return starRocksClientConverter.convertToNode(table, TableConfig.builder().build());
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
