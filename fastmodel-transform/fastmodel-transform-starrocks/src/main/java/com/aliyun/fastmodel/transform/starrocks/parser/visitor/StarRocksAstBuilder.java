package com.aliyun.fastmodel.transform.starrocks.parser.visitor;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentTimestamp;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DoubleLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.misc.EmptyStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import com.aliyun.fastmodel.transform.starrocks.format.TimeFunctionType;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksBaseVisitor;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.AggDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.ArrayTypeContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.BackQuotedIdentifierContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.BaseTypeContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.BooleanLiteralContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.ColumnDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.CommentContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.CreateTableStatementContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.DecimalTypeContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.DecimalValueContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.DefaultDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.DigitIdentifierContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.DistributionDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.DoubleValueContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.DupKeysContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.EngineDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.FromRollupContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.IdentifierListContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.IndexDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.IndexTypeContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.IntegerValueContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.IntervalContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.KeyDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.MapTypeContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.MultiItemListPartitionDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.MultiRangePartitionContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.NullLiteralContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.NumericLiteralContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.OrderByDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.PartitionDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.PartitionKeyDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.PartitionListIdentifierContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.PartitionRangeIdentifierContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.PartitionValueContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.PartitionValueListContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.PropertiesContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.PropertyContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.QualifiedNameContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.RollupDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.RollupItemContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.SingleItemListPartitionDescContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.SingleRangePartitionContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.SingleStatementContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.SqlStatementsContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.StringContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.StringLiteralContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.TypeContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.TypeListContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.TypeParameterContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksParser.UnquotedIdentifierContext;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.OrderByConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.RollupConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.RollupItem;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksDataTypeName;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksGenericDataType;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiItemListPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiRangePartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionDesc;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleItemListPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleRangePartition;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.BooleanUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;

/**
 * StarRocksAstBuilder
 *
 * @author panguanjing
 * @date 2023/9/11
 */
public class StarRocksAstBuilder extends StarRocksBaseVisitor<Node> {

    private final ReverseContext reverseContext;

    public StarRocksAstBuilder(ReverseContext context) {
        this.reverseContext = context;
    }

    @Override
    public Node visitSqlStatements(SqlStatementsContext ctx) {
        List<SingleStatementContext> sqlStatementContexts = ctx.singleStatement();
        List<BaseStatement> list = new ArrayList<>();
        if (sqlStatementContexts == null) {
            throw new ParseException("can't parse the empty context");
        }
        if (sqlStatementContexts.size() == 1) {
            return visit(sqlStatementContexts.get(0));
        }
        for (SingleStatementContext sqlStatementContext : sqlStatementContexts) {
            Node node = visit(sqlStatementContext);
            list.add((BaseStatement)node);
        }
        return new CompositeStatement(getLocation(ctx), getOrigin(ctx), list);
    }

    @Override
    public Node visitSingleStatement(SingleStatementContext ctx) {
        if (ctx.statement() != null) {
            return visit(ctx.statement());
        }
        if (ctx.emptyStatement() != null) {
            return new EmptyStatement();
        }
        return super.visitSingleStatement(ctx);
    }

    @Override
    public Node visitCreateTableStatement(CreateTableStatementContext ctx) {
        // table name
        QualifiedNameContext qualifiedNameContext = ctx.qualifiedName();
        QualifiedName qualifiedName = (QualifiedName)visit(qualifiedNameContext);
        // comment
        CommentContext comment = ctx.comment();
        Optional<Comment> tableComment = ParserHelper.visitIfPresent(this, comment, Comment.class);
        //columns
        List<ColumnDefinition> columns = ParserHelper.visit(this, ctx.columnDesc(), ColumnDefinition.class);
        //index
        List<TableIndex> listTableIndex = ParserHelper.visit(this, ctx.indexDesc(), TableIndex.class);
        //constraint
        Optional<BaseConstraint> list = ParserHelper.visitIfPresent(this, ctx.keyDesc(), BaseConstraint.class);
        List<BaseConstraint> constraints = Lists.newArrayList();
        list.ifPresent(constraints::add);

        //distrbiute key constraint
        if (ctx.distributionDesc() != null) {
            DistributeConstraint distributeKeyConstraint = (DistributeConstraint)visit(ctx.distributionDesc());
            constraints.add(distributeKeyConstraint);
        }
        //rollup_index
        if (ctx.rollupDesc() != null) {
            RollupConstraint rollupConstraint = (RollupConstraint)visit(ctx.rollupDesc());
            constraints.add(rollupConstraint);
        }
        //order by constraint
        if (ctx.orderByDesc() != null) {
            OrderByConstraint orderByConstraint = (OrderByConstraint)visit(ctx.orderByDesc());
            constraints.add(orderByConstraint);
        }
        PropertiesContext properties = ctx.properties();
        List<Property> propertyList = Lists.newArrayList();
        if (properties != null) {
            propertyList = ParserHelper.visit(this, properties.property(), Property.class);
        }
        //extend properties
        List<Property> extendProperties = toExtend(ctx);
        List<Property> all = Lists.newArrayList();
        if (propertyList != null) {
            all.addAll(propertyList);
        }
        all.addAll(extendProperties);

        //partition by
        PartitionDescContext partitionDescContext = ctx.partitionDesc();
        PartitionedBy partitionedBy = null;
        if (partitionDescContext != null) {
            partitionedBy = (PartitionedBy)visit(partitionDescContext);
        }
        return CreateTable.builder()
            .tableName(qualifiedName)
            .tableIndex(listTableIndex)
            .columns(columns)
            .constraints(constraints)
            .comment(tableComment.orElse(null))
            .properties(all)
            .partition(partitionedBy)
            .build();
    }

    @Override
    public Node visitIndexDesc(IndexDescContext ctx) {
        //index desc
        Identifier indexName = (Identifier)visit(ctx.indexName);
        List<Identifier> indexColumns = ParserHelper.visit(this, ctx.identifierList().identifier(), Identifier.class);
        List<Property> properties = Lists.newArrayList();
        IndexTypeContext indexTypeContext = ctx.indexType();
        if (indexTypeContext != null) {
            Property property = new Property(StarRocksProperty.TABLE_INDEX_TYPE.getValue(), "BITMAP");
            properties.add(property);
        }
        if (ctx.comment() != null) {
            Comment comment = (Comment)visit(ctx.comment());
            Property property = new Property(StarRocksProperty.TABLE_INDEX_COMMENT.getValue(), comment.getComment());
            properties.add(property);
        }
        return new TableIndex(
            indexName,
            indexColumns.stream().map(i -> new IndexColumnName(i, null, null)).collect(Collectors.toList()),
            properties
        );
    }

    @Override
    public Node visitPartitionRangeIdentifier(PartitionRangeIdentifierContext ctx) {
        IdentifierListContext identifierListContext = ctx.identifierList();
        List<Identifier> list = ParserHelper.visit(this, identifierListContext.identifier(), Identifier.class);
        List<ColumnDefinition> columnDefinitionList =
            list.stream().map(c -> ColumnDefinition.builder().colName(c).build()).collect(Collectors.toList());
        List<PartitionDesc> rangePartitions = ParserHelper.visit(this, ctx.rangePartitionDesc(), PartitionDesc.class);
        return new RangePartitionedBy(
            columnDefinitionList, rangePartitions
        );
    }

    @Override
    public Node visitPartitionListIdentifier(PartitionListIdentifierContext ctx) {
        List<Identifier> visit = ParserHelper.visit(this, ctx.identifierList().identifier(), Identifier.class);
        List<ColumnDefinition> columnDefines = visit.stream().map(
            i -> ColumnDefinition.builder()
                .colName(i)
                .build()
        ).collect(Collectors.toList());
        List<PartitionDesc> rangePartitions = null;
        if (ctx.listPartitionDesc() != null) {
            rangePartitions = ParserHelper.visit(this, ctx.listPartitionDesc(), PartitionDesc.class);
        }
        return new ListPartitionedBy(columnDefines, rangePartitions);
    }

    @Override
    public Node visitPartitionFunctionCall(StarRocksParser.PartitionFunctionCallContext ctx) {
        return visit(ctx.functionCall());
    }

    @Override
    public Node visitSimpleFunctionCall(StarRocksParser.SimpleFunctionCallContext ctx) {
        QualifiedName funcName = (QualifiedName)visit(ctx.qualifiedName());

        Identifier columnName = null;
        FunctionCall functionCall = null;
        if (TimeFunctionType.DATE_TRUNC.getValue().equalsIgnoreCase(funcName.getFirst())) {
            StringLiteral timeUnit = (StringLiteral)visit(ctx.expression(0));
            List<BaseExpression> arguments = Lists.newArrayList(timeUnit);
            functionCall = new FunctionCall(funcName, false, arguments);
            columnName = (Identifier)visit(ctx.expression(1));
        } else if (TimeFunctionType.TIME_SLICE.getValue().equalsIgnoreCase(funcName.getFirst())) {
            columnName = (Identifier)visit(ctx.expression(0));
            IntervalLiteral interval = (IntervalLiteral)visit(ctx.expression(1));
            List<BaseExpression> arguments = Lists.newArrayList(interval);
            functionCall = new FunctionCall(funcName, false, arguments);
        } else {
            // 目前仅支持这两种函数
            return null;
        }

        ColumnDefinition columnDefinition = ColumnDefinition.builder()
                .colName(columnName)
                .build();

        return new ExpressionPartitionBy(Lists.newArrayList(columnDefinition), functionCall, null);
    }

    @Override
    public Node visitPartitionListIdentiifer(StarRocksParser.PartitionListIdentiiferContext ctx) {
        IdentifierListContext identifierListContext = ctx.identifierList();
        List<Identifier> list = ParserHelper.visit(this, identifierListContext.identifier(), Identifier.class);
        List<ColumnDefinition> columnDefinitions = list.stream().map(identifier -> ColumnDefinition.builder()
                .colName(identifier)
                .build()).collect(Collectors.toList());
        return new ExpressionPartitionBy(columnDefinitions, null, null);
    }

    @Override
    public Node visitSingleItemListPartitionDesc(SingleItemListPartitionDescContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        ListStringLiteral listStringLiteral = (ListStringLiteral)visit(ctx.stringList());
        List<Property> properList = null;
        if (ctx.propertyList() != null) {
            properList = ParserHelper.visit(this, ctx.propertyList().property(), Property.class);
        }
        return new SingleItemListPartition(identifier, ctx.IF() != null, listStringLiteral, properList);
    }

    @Override
    public Node visitStringList(StarRocksParser.StringListContext ctx) {
        List<StringLiteral> stringLiteralList = ctx.string().stream()
                .map(value -> new StringLiteral(value.getText())).collect(Collectors.toList());
        return new ListStringLiteral(getLocation(ctx),
                getOrigin(ctx), stringLiteralList);
    }

    @Override
    public Node visitMultiItemListPartitionDesc(MultiItemListPartitionDescContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        List<ListStringLiteral> listStringLiteral = ParserHelper.visit(this, ctx.stringList(), ListStringLiteral.class);
        List<Property> properList = null;
        if (ctx.propertyList() != null) {
            properList = ParserHelper.visit(this, ctx.propertyList().property(), Property.class);
        }
        return new MultiItemListPartition(identifier, ctx.IF() != null, listStringLiteral, properList);
    }

    @Override
    public Node visitSingleRangePartition(SingleRangePartitionContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        PartitionKey partitionKey = (PartitionKey)visit(ctx.partitionKeyDesc());
        List<Property> propertyList = null;
        if (ctx.propertyList() != null) {
            propertyList = ParserHelper.visit(this, ctx.propertyList().property(), Property.class);
        }
        return new SingleRangePartition(identifier, ctx.IF() != null, partitionKey, propertyList);
    }

    @Override
    public Node visitMultiRangePartition(MultiRangePartitionContext ctx) {
        StringLiteral start = (StringLiteral)visit(ctx.start);
        StringLiteral end = (StringLiteral)visit(ctx.end);
        IntervalLiteral intervalLiteral = null;
        LongLiteral longLiteral = null;
        if (ctx.INTEGER_VALUE() != null) {
            longLiteral = new LongLiteral(ctx.INTEGER_VALUE().getText());
        } else {
            intervalLiteral = (IntervalLiteral)visit(ctx.interval());
        }
        return new MultiRangePartition(start, end, intervalLiteral, longLiteral);
    }

    @Override
    public Node visitPartitionKeyDesc(PartitionKeyDescContext ctx) {
        if (ctx.LESS() != null) {
            ListPartitionValue visit = (ListPartitionValue)visit(ctx.partitionValueList().get(0));
            return new LessThanPartitionKey(
                ctx.MAXVALUE() != null,
                visit
            );
        } else {
            List<ListPartitionValue> list = ParserHelper.visit(this, ctx.partitionValueList(), ListPartitionValue.class);
            return new ArrayPartitionKey(list);
        }
    }

    @Override
    public Node visitPartitionValueList(PartitionValueListContext ctx) {
        List<PartitionValue> list = ParserHelper.visit(this, ctx.partitionValue(), PartitionValue.class);
        return new ListPartitionValue(
            getLocation(ctx),
            list
        );
    }

    @Override
    public Node visitPartitionValue(PartitionValueContext ctx) {
        StringLiteral stringLiteral = null;
        if (ctx.string() != null) {
            stringLiteral = (StringLiteral)visit(ctx.string());
        }
        return new PartitionValue(
            ctx.MAXVALUE() != null,
            stringLiteral
        );
    }

    @Override
    public Node visitDistributionDesc(DistributionDescContext ctx) {
        Integer bucket = null;
        if (ctx.BUCKETS() != null) {
            bucket = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        }
        if (ctx.RANDOM() != null) {
            return new DistributeConstraint(true, bucket);
        }
        if (ctx.identifierList() != null) {
            return new DistributeConstraint(
                ParserHelper.visit(this, ctx.identifierList().identifier(), Identifier.class),
                bucket
            );
        }
        return super.visitDistributionDesc(ctx);
    }

    private List<Property> toExtend(CreateTableStatementContext ctx) {
        List<Property> list = Lists.newArrayList();
        EngineDescContext engineDescContext = ctx.engineDesc();
        if (engineDescContext != null) {
            Property property = (Property)visit(engineDescContext);
            list.add(property);
        }
        return list;
    }

    @Override
    public Node visitArrayType(ArrayTypeContext ctx) {
        BaseDataType baseDataType = (BaseDataType)visit(ctx.type());
        return new StarRocksGenericDataType(
            StarRocksDataTypeName.ARRAY.getValue(),
            Lists.newArrayList(new TypeParameter(baseDataType))
        );
    }

    @Override
    public Node visitRollupDesc(RollupDescContext ctx) {
        List<RollupItem> rollupItemList = ParserHelper.visit(this, ctx.rollupItem(), RollupItem.class);
        return new RollupConstraint(rollupItemList);
    }

    @Override
    public Node visitRollupItem(RollupItemContext ctx) {
        List<Identifier> identifiers = ParserHelper.visit(this, ctx.identifierList().identifier(), Identifier.class);
        DupKeysContext dupKeysContext = ctx.dupKeys();
        List<Identifier> duplicateKeyList = null;
        if (dupKeysContext != null) {
            duplicateKeyList = ParserHelper.visit(this, ctx.dupKeys().identifierList().identifier(), Identifier.class);
        }
        Identifier fromRollup = ParserHelper.visitIfPresent(this, ctx.fromRollup(), Identifier.class).orElse(null);
        List<Property> properties = null;
        if (ctx.properties() != null) {
            properties = ParserHelper.visit(this, ctx.properties().property(), Property.class);
        }
        return new RollupItem(
            (Identifier)visit(ctx.rollupName),
            identifiers,
            duplicateKeyList,
            fromRollup,
            properties
        );
    }

    @Override
    public Node visitFromRollup(FromRollupContext ctx) {
        return visit(ctx.identifier());
    }

    @Override
    public Node visitEngineDesc(EngineDescContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        return new Property(StarRocksProperty.TABLE_ENGINE.getValue(), identifier.getValue());
    }

    @Override
    public Node visitKeyDesc(KeyDescContext ctx) {
        IdentifierListContext identifierListContext = ctx.identifierList();
        List<Identifier> list = ParserHelper.visit(this, identifierListContext.identifier(), Identifier.class);
        if (ctx.AGGREGATE() != null) {
            return new AggregateKeyConstraint(IdentifierUtil.sysIdentifier(), list, true);
        }
        if (ctx.PRIMARY() != null) {
            return new PrimaryConstraint(IdentifierUtil.sysIdentifier(), list);
        }
        if (ctx.UNIQUE() != null) {
            return new UniqueConstraint(IdentifierUtil.sysIdentifier(), list);
        }
        if (ctx.DUPLICATE() != null) {
            return new DuplicateKeyConstraint(IdentifierUtil.sysIdentifier(), list, true);
        }
        return super.visitKeyDesc(ctx);
    }

    @Override
    public Node visitOrderByDesc(OrderByDescContext ctx) {
        List<Identifier> identifierList = ParserHelper.visit(this, ctx.identifierList().identifier(), Identifier.class);
        return new OrderByConstraint(
            IdentifierUtil.sysIdentifier(),
            identifierList
        );
    }

    @Override
    public Node visitColumnDesc(ColumnDescContext ctx) {
        //column name
        Identifier identifier = (Identifier)visit(ctx.identifier());
        TypeContext type = ctx.type();
        //data type
        BaseDataType baseDataType = (BaseDataType)visit(type);
        //comment
        Optional<Comment> comment = ParserHelper.visitIfPresent(this, ctx.comment(), Comment.class);
        //not null
        Boolean notNull = null;
        if (ctx.NOT() != null && ctx.NULL() != null) {
            notNull = true;
        } else if (ctx.NULL() != null) {
            notNull = false;
        }
        //default value
        DefaultDescContext defaultDescContext = ctx.defaultDesc();
        BaseExpression baseLiteral = null;
        if (defaultDescContext != null) {
            baseLiteral = (BaseExpression)visit(ctx.defaultDesc());
        }

        //agg desc
        AggDescContext aggDescContext = ctx.aggDesc();
        List<Property> properties = Lists.newArrayList();
        if (aggDescContext != null) {
            Property property = new Property(StarRocksProperty.COLUMN_AGG_DESC.getValue(), aggDescContext.getText());
            properties.add(property);
        }

        if (ctx.charsetName() != null) {
            Identifier value = (Identifier)visit(ctx.charsetName().identifier());
            Property property = new Property(StarRocksProperty.COLUMN_CHAR_SET.getValue(), value.getValue());
            properties.add(property);
        }
        if (ctx.KEY() != null) {
            Property property = new Property(StarRocksProperty.COLUMN_KEY.getValue(), "KEY");
            properties.add(property);
        }

        if (ctx.AUTO_INCREMENT() != null) {
            Property property = new Property(StarRocksProperty.COLUMN_AUTO_INCREMENT.getValue(), BooleanUtils.toStringTrueFalse(true));
            properties.add(property);
        }
        return ColumnDefinition.builder()
            .dataType(baseDataType)
            .comment(comment.orElse(null))
            .colName(identifier)
            .properties(properties)
            .notNull(notNull)
            .defaultValue(baseLiteral)
            .build();
    }

    @Override
    public Node visitDefaultDesc(DefaultDescContext ctx) {
        if (ctx.string() != null) {
            return visit(ctx.string());
        }
        if (ctx.NULL() != null) {
            return new NullLiteral();
        }
        if (ctx.CURRENT_TIMESTAMP() != null) {
            return new CurrentTimestamp();
        }
        if (ctx.qualifiedName() != null) {
            QualifiedName qualifiedName = (QualifiedName)visit(ctx.qualifiedName());
            return new FunctionCall(qualifiedName, false, Lists.newArrayList());
        }
        return super.visitDefaultDesc(ctx);
    }

    @Override
    public Node visitTypeList(TypeListContext ctx) {
        return super.visitTypeList(ctx);
    }

    @Override
    public Node visitProperty(PropertyContext ctx) {
        StringLiteral key = (StringLiteral)visit(ctx.key);
        StringLiteral value = (StringLiteral)visit(ctx.value);
        return new Property(key.getValue(), value);
    }

    @Override
    public Node visitMapType(MapTypeContext ctx) {
        return super.visitMapType(ctx);
    }

    @Override
    public Node visitBaseType(BaseTypeContext ctx) {
        Token name = ctx.name;
        IDataTypeName byValue = StarRocksDataTypeName.getByValue(name.getText());
        List<DataTypeParameter> list = Lists.newArrayList();
        if (ctx.typeParameter() != null) {
            DataTypeParameter dataTypeParameter = (DataTypeParameter)visit(ctx.typeParameter());
            list.add(dataTypeParameter);
        }
        return new StarRocksGenericDataType(getLocation(ctx), getOrigin(ctx), byValue.getValue(), list);
    }

    @Override
    public Node visitTypeParameter(TypeParameterContext ctx) {
        return new NumericParameter(ctx.INTEGER_VALUE().getText());
    }

    @Override
    public Node visitDecimalType(DecimalTypeContext ctx) {
        Token name = ctx.name;
        IDataTypeName byValue = StarRocksDataTypeName.getByValue(name.getText());
        List<DataTypeParameter> list = Lists.newArrayList();
        if (ctx.precision != null) {
            DataTypeParameter p = new NumericParameter(ctx.precision.getText());
            list.add(p);
        }
        if (ctx.scale != null) {
            DataTypeParameter p = new NumericParameter(ctx.scale.getText());
            list.add(p);
        }
        return new StarRocksGenericDataType(byValue.getValue(), list);
    }

    @Override
    public Node visitComment(CommentContext ctx) {
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.string());
        return new Comment(stringLiteral.getValue());
    }

    @Override
    public Node visitString(StringContext ctx) {
        return new StringLiteral(
            getLocation(ctx),
            getOrigin(ctx),
            StripUtils.strip(ctx.getText()));
    }

    @Override
    public Node visitInterval(IntervalContext ctx) {
        BaseLiteral baseLiteral = (BaseLiteral)visit(ctx.value);
        DateTimeEnum dateTimeEnum = DateTimeEnum.getByCode(ctx.from.getText());
        return new IntervalLiteral(baseLiteral, dateTimeEnum);
    }

    @Override
    public Node visitNullLiteral(NullLiteralContext ctx) {
        return new NullLiteral();
    }

    @Override
    public Node visitBooleanLiteral(BooleanLiteralContext ctx) {
        return new BooleanLiteral(ctx.booleanValue().getText());
    }

    @Override
    public Node visitNumericLiteral(NumericLiteralContext ctx) {
        return visit(ctx.number());
    }

    @Override
    public Node visitDecimalValue(DecimalValueContext ctx) {
        return new DecimalLiteral(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), ctx.getText());
    }

    @Override
    public Node visitDoubleValue(DoubleValueContext ctx) {
        return new DoubleLiteral(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), ctx.getText());
    }

    @Override
    public Node visitIntegerValue(IntegerValueContext ctx) {
        return new LongLiteral(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), ctx.getText());
    }

    @Override
    public Node visitStringLiteral(StringLiteralContext ctx) {
        return visit(ctx.string());
    }

    @Override
    public Node visitQualifiedName(QualifiedNameContext ctx) {
        List<Identifier> list = ParserHelper.visit(this, ctx.identifier(), Identifier.class);
        return QualifiedName.of(list);
    }

    @Override
    public Node visitUnquotedIdentifier(UnquotedIdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitDigitIdentifier(DigitIdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitBackQuotedIdentifier(BackQuotedIdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }
}
