/*
 * Copyright 2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.mc.parser.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.semantic.table.CreateTableSemanticCheck;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.JsonDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableType;
import com.aliyun.fastmodel.core.tree.statement.select.order.Ordering;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateOdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.AutoPartitionedBy;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.AutoTablePartitionContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.ColumnNameColonTypeContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.ColumnNameTypeConstraintContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.CommentClauseContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.ConstraintsContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.CreateTableStatementContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.DefaultValueContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.EnableSpecContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.IdentifierContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.JsonTypeContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.KeyValuePropertyContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.ListTypeContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.MapTypeContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.NamedExpressionContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.NullableSpecContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.OutOfLineConstraintsContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.PartitionColumnNameTypeContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.PrimitiveTypeContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.QueryStatementContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.ScriptContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.StatementContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.StructTypeContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.TableLifecycleContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.TableNameContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.TablePartitionContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.VectorTypeContext;
import com.aliyun.fastmodel.transform.mc.parser.tree.constraint.ClusteredByConstraint;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeGenericDataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;

/**
 * 用于解析
 *
 * @author panguanjing
 * @date 2020/11/25
 */
public class MaxComputeAstBuilder extends MaxComputeExpressionAstBuilder {

    private final ReverseContext context;

    private final FastModelParser fastModelParser;

    public MaxComputeAstBuilder(ReverseContext context) {
        this.context = context == null ? ReverseContext.builder().build() : context;
        fastModelParser = FastModelParserFactory.getInstance().get();
    }

    @Override
    public Node visitScript(ScriptContext ctx) {
        List<BaseStatement> compositeStatement = visit(ctx.statement(), BaseStatement.class);
        return new CompositeStatement(compositeStatement);
    }

    @Override
    public Node visitStatement(StatementContext ctx) {
        if (ctx.explainStatement() != null) {
            return visit(ctx.explainStatement());
        }
        if (ctx.execStatement() != null) {
            return visit(ctx.execStatement());
        }
        if (ctx.setStatement() != null) {
            return visit(ctx.setStatement());
        }
        if (ctx.setProjectStatement() != null) {
            return visit(ctx.setProjectStatement());
        }
        return super.visitStatement(ctx);
    }

    @Override
    public Node visitQueryStatement(QueryStatementContext ctx) {
        String origin = getOrigin(ctx);
        return fastModelParser.parseStatement(origin);
    }

    @Override
    public Node visitCommentClause(CommentClauseContext ctx) {
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.comment);
        return new Comment(getLocation(ctx), stringLiteral.getValue());
    }

    @Override
    public Node visitCreateTableStatement(CreateTableStatementContext ctx) {
        // 由用户指定路由的context的key内容
        TableDetailType tableDetailType = context.getReverseTableType();
        if (tableDetailType == null) {
            tableDetailType = TableDetailType.NORMAL_DIM;
        }
        // 表名
        Table table = (Table)visit(ctx.name);
        // 表property
        List<Property> defaultProperties = ImmutableList.of();
        if (ctx.prop != null) {
            defaultProperties = visit(ctx.prop.tableProperties().tablePropertiesList().kv, Property.class);
        }
        Property lifeCycleProperty = getLifeCycleProp(ctx.lifecycle);
        List<Property> allProperties = Lists.newArrayList(context.getProperties() == null ? Lists.newArrayList() : context.getProperties());
        allProperties.addAll(defaultProperties);
        if (lifeCycleProperty != null) {
            allProperties.add(lifeCycleProperty);
        }
        // 外表属性
        buildExternalProperties(ctx, allProperties);
        // 表constraint
        List<BaseConstraint> constraints = new ArrayList<>();
        Optional<PrimaryConstraint> primaryConstraint = visitIfPresent(ctx.outOfLineConstraints(),
            PrimaryConstraint.class);
        primaryConstraint.ifPresent(constraints::add);
        Optional<ClusteredByConstraint> clusteredByConstraint = visitIfPresent(ctx.tableBuckets(),
            ClusteredByConstraint.class);
        clusteredByConstraint.ifPresent(constraints::add);

        // all columns
        List<ColumnDefinition> allColumn = Lists.newArrayList();
        List<ColumnDefinition> columnDefinitions = ImmutableList.of();
        if (ctx.columnNameTypeConstraintList() != null) {
            columnDefinitions = visit(ctx.columnNameTypeConstraintList().columnNameTypeConstraint(),
                ColumnDefinition.class);
            allColumn.addAll(columnDefinitions);
        }

        TablePartitionContext partitions = ctx.tablePartition();
        PartitionedBy partitionedBy = null;
        if (partitions != null) {
            List<ColumnDefinition> list = visit(partitions.partitionColumnNameTypeList().partitionColumnNameType(),
                ColumnDefinition.class);
            partitionedBy = new PartitionedBy(list);
            allColumn.addAll(list);
        } else if (ctx.autoPartition != null) {
            partitionedBy = (PartitionedBy)visit(ctx.autoPartition);
        }

        // check column has same column
        CreateTableSemanticCheck.containSameCol(allColumn);

        // if primary constraint not exist
        if (!primaryConstraint.isPresent()) {
            List<ColumnDefinition> primaryColumns = allColumn.stream()
                .filter(x -> BooleanUtils.isTrue(x.getPrimary()))
                .collect(Collectors.toList());
            if (!primaryColumns.isEmpty()) {
                PrimaryConstraint keyPrimaryConstraint = new PrimaryConstraint(
                    IdentifierUtil.sysIdentifier(),
                    primaryColumns.stream()
                        .map(ColumnDefinition::getColName)
                        .collect(Collectors.toList())
                );
                constraints.add(keyPrimaryConstraint);
                removeColumnPrimary(primaryColumns, columnDefinitions, partitionedBy);
            }
        }

        CommentClauseContext tree = ctx.commentClause();
        boolean ifNotExist = ctx.ifNotExists() != null;
        Comment comment = visitIfPresent(tree, Comment.class).orElse(null);
        TableBuilder tableBuilder = null;
        if (tableDetailType.getParent() == TableType.ODS) {
            tableBuilder = CreateOdsTable.builder();
        } else if (tableDetailType.getParent() == TableType.DIM) {
            tableBuilder = CreateDimTable.builder();
        } else if (tableDetailType.getParent() == TableType.FACT) {
            tableBuilder = CreateFactTable.builder();
        } else if (tableDetailType.getParent() == TableType.DWS) {
            tableBuilder = CreateDwsTable.builder();
        } else if (tableDetailType.getParent() == TableType.ADS) {
            tableBuilder = CreateAdsTable.builder();
        }
        if (tableBuilder == null) {
            throw new UnsupportedOperationException("unsupported create table with detailType:" + tableDetailType);
        }
        return tableBuilder.tableName(table.getName())
            .detailType(tableDetailType)
            .ifNotExist(ifNotExist)
            .columns(columnDefinitions)
            .constraints(constraints)
            .properties(allProperties)
            .comment(comment)
            .partition(partitionedBy).build();
    }

    /**
     * 只保留一个主键
     *
     * @param primaryColumns
     * @param columnDefinitions
     * @param partitionedBy
     */
    private void removeColumnPrimary(List<ColumnDefinition> primaryColumns, List<ColumnDefinition> columnDefinitions, PartitionedBy partitionedBy) {
        Set<Identifier> column = primaryColumns.stream().map(ColumnDefinition::getColName).collect(Collectors.toSet());
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            if (column.contains(columnDefinition.getColName())) {
                columnDefinition.setPrimary(null);
            }
        }
        if (partitionedBy == null) {
            return;
        }
        for (ColumnDefinition columnDefinition : partitionedBy.getColumnDefinitions()) {
            if (column.contains(columnDefinition.getColName())) {
                columnDefinition.setPrimary(null);
            }
        }
    }

    private Property getLifeCycleProp(TableLifecycleContext lifecycle) {
        if (lifecycle == null) {
            return null;
        }
        String text = lifecycle.lifecycleDays.getText();
        return new Property(
            MaxComputePropertyKey.LIFE_CYCLE.getValue(),
            text
        );
    }

    @Override
    public Node visitOutOfLineConstraints(OutOfLineConstraintsContext ctx) {
        List<Identifier> cols = visit(ctx.c.columnName(), Identifier.class);
        EnableSpecContext e = ctx.e;
        boolean enable = true;
        if (e != null) {
            enable = e.KW_DISABLE() == null || e.KW_ENABLE() != null;
        }
        return new PrimaryConstraint(
            visitIfPresent(ctx.identifier(), Identifier.class).orElse(IdentifierUtil.sysIdentifier()),
            cols,
            enable
        );
    }

    @Override
    public Node visitColumnNameType(OdpsParser.ColumnNameTypeContext ctx) {
        Comment comment = getComment(ctx.commentClause());
        Identifier visit = (Identifier)visit(ctx.colName);
        ColumnDefinition columnDefinition = context.getColumnProperties().get(visit);
        if (columnDefinition == null) {
            columnDefinition = ColumnDefinition.builder().colName(visit).build();
        }
        return ColumnDefinition.builder().colName(visit)
            .dataType((BaseDataType)visit(ctx.colType()))
            .comment(comment)
            .primary(columnDefinition.getPrimary())
            .aliasedName(columnDefinition.getAliasedName())
            .properties(columnDefinition.getColumnProperties())
            .category(columnDefinition.getCategory())
            .refDimension(columnDefinition.getRefDimension())
            .refIndicators(columnDefinition.getRefIndicators())
            .build();

    }

    @Override
    public Node visitAutoTablePartition(AutoTablePartitionContext ctx) {
        NamedExpressionContext namedExpressionContext = ctx.namedExpression();
        IdentifierContext alias = namedExpressionContext.alias;
        Identifier aliasIdentifier = alias == null ? null : (Identifier)visit(alias);
        BaseExpression expression = (BaseExpression)visit(namedExpressionContext.expression());
        return new AutoPartitionedBy(List.of(), expression, aliasIdentifier);
    }

    private Comment getComment(CommentClauseContext ctx) {
        if (ctx == null) {
            return null;
        }
        Optional<StringLiteral> optionalStringLiteral = visitIfPresent(ctx.stringLiteral(), StringLiteral.class);
        Comment comment = null;
        if (optionalStringLiteral.isPresent()) {
            comment = new Comment(optionalStringLiteral.get().getValue());
        }
        return comment;
    }

    @Override
    public ClusteredByConstraint visitTableBuckets(OdpsParser.TableBucketsContext ctx) {
        ClusteredByConstraint.ClusterType clusterType = ctx.KW_RANGE() != null
            ? ClusteredByConstraint.ClusterType.RANGE
            : ClusteredByConstraint.ClusterType.HASH;

        List<Identifier> columns = null;
        List<SortItem> sortColumns = null;
        LongLiteral bucketNum = null;
        if (ctx.clusterColumnNameOrderList() != null
            && CollectionUtils.isNotEmpty(ctx.clusterColumnNameOrderList().columnNameOrder())) {
            List<OdpsParser.ColumnNameOrderContext> columnNameOrderContexts = ctx.clusterColumnNameOrderList().columnNameOrder();
            columns = columnNameOrderContexts.stream()
                .map(columnNameOrder -> new Identifier(getLocation(columnNameOrder), getOrigin(columnNameOrder), StripUtils.strip(columnNameOrder.getText())))
                .collect(Collectors.toList());
        }
        if (ctx.columnNameOrderList() != null
            && CollectionUtils.isNotEmpty(ctx.columnNameOrderList().columnNameOrder())) {
            List<OdpsParser.ColumnNameOrderContext> columnNameOrderContexts = ctx.columnNameOrderList().columnNameOrder();
            sortColumns = columnNameOrderContexts.stream()
                .map(columnNameOrder -> {
                    IdentifierContext identifier = columnNameOrder.identifier();
                    StringLiteral stringLiteral = new StringLiteral(getLocation(identifier), getOrigin(identifier), StripUtils.strip(identifier.getText()));
                    Ordering ordering = columnNameOrder.KW_DESC() != null ? Ordering.DESC : Ordering.ASC;
                    return new SortItem(stringLiteral, ordering, null);
                }).collect(Collectors.toList());
        }
        if (ctx.Number() != null) {
            TerminalNode number = ctx.Number();
            bucketNum = new LongLiteral(getLocation(number), getOrigin(number), StripUtils.strip(number.getText()));
        }
        return new ClusteredByConstraint(columns, clusterType, sortColumns, bucketNum);
    }

    @Override
    public Node visitPartitionColumnNameType(PartitionColumnNameTypeContext ctx) {
        Identifier colName = (Identifier)visit(ctx.colName);
        BaseDataType baseDataType = (BaseDataType)visit(ctx.colType());
        Boolean notNull = null;
        if (ctx.nullableSpec() != null) {
            notNull = ctx.nullableSpec().KW_NOT() != null;
        }
        Comment c = getComment(ctx.commentClause());
        ColumnDefinition columnDefinition = context.getColumnProperties().get(colName);
        if (columnDefinition == null) {
            columnDefinition = ColumnDefinition.builder().colName(colName).build();
        }
        return ColumnDefinition.builder()
            .colName(colName)
            .dataType(baseDataType)
            .notNull(notNull)
            .primary(columnDefinition.getPrimary())
            .aliasedName(columnDefinition.getAliasedName())
            .properties(columnDefinition.getColumnProperties())
            .category(columnDefinition.getCategory())
            .refDimension(columnDefinition.getRefDimension())
            .refIndicators(columnDefinition.getRefIndicators())
            .comment(c)
            .build();
    }

    @Override
    public Node visitColumnNameTypeConstraint(ColumnNameTypeConstraintContext ctx) {
        Comment comment = getComment(ctx.commentClause());
        Boolean primary = null;
        Boolean notNull = null;
        List<ConstraintsContext> constraints = ctx.constraints();
        BaseLiteral baseLiteral = null;
        if (constraints != null) {
            for (ConstraintsContext constraintsContext : constraints) {
                if (constraintsContext.p != null) {
                    primary = true;
                }
                if (constraintsContext.n != null && constraintsContext.n.KW_NOT() != null) {
                    notNull = true;
                }
                if (constraintsContext.d != null) {
                    baseLiteral = (BaseLiteral)visit(constraintsContext.d);
                }
            }
        }
        Identifier columnName = (Identifier)visit(ctx.identifier());
        ColumnDefinition columnDefinition = context.getColumnProperties().get(columnName);
        if (columnDefinition == null) {
            columnDefinition = ColumnDefinition.builder().colName(columnName).build();
        }
        return ColumnDefinition.builder()
            .colName(columnName)
            .aliasedName(columnDefinition.getAliasedName())
            .dataType((BaseDataType)visit(ctx.colType()))
            .comment(comment)
            .primary(primary == null ? columnDefinition.getPrimary() : primary)
            .notNull(notNull)
            .defaultValue(baseLiteral)
            .refIndicators(columnDefinition.getRefIndicators())
            .refDimension(columnDefinition.getRefDimension())
            .category(columnDefinition.getCategory())
            .properties(columnDefinition.getColumnProperties())
            .build();

    }

    @Override
    public Node visitDefaultValue(DefaultValueContext ctx) {
        return visit(ctx.constant());
    }

    @Override
    public Node visitColType(OdpsParser.ColTypeContext ctx) {
        return visit(ctx.type());
    }

    @Override
    public Node visitPrimitiveType(PrimitiveTypeContext ctx) {
        Identifier name = new Identifier(ctx.name.getText());
        List<DataTypeParameter> list = new ArrayList<>();
        if (ctx.length != null) {
            DataTypeParameter dataTypeParameter = new NumericParameter(ctx.length.getText());
            list.add(dataTypeParameter);
        }
        if (ctx.prec != null) {
            DataTypeParameter dataTypeParameter = new NumericParameter(ctx.prec.getText());
            list.add(dataTypeParameter);

        }
        if (ctx.scale != null) {
            DataTypeParameter dataTypeParameter = new NumericParameter(ctx.scale.getText());
            list.add(dataTypeParameter);
        }
        return new MaxComputeGenericDataType(name, list);
    }

    @Override
    public Node visitJsonType(JsonTypeContext ctx) {
        if (ctx.columnNameColonTypeList() != null) {
            List<Field> list = visit(ctx.columnNameColonTypeList().columnNameColonType(), Field.class);
            return new JsonDataType(getLocation(ctx), getOrigin(ctx), list);
        }
        return new JsonDataType(getLocation(ctx), getOrigin(ctx), Lists.newArrayList());
    }

    @Override
    public Node visitVectorType(VectorTypeContext ctx) {
        BaseDataType elemType = (BaseDataType)visit(ctx.elemType);
        validateVectorType(elemType, ctx.dim.getText());
        return new MaxComputeGenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            ctx.KW_VECTOR().getText(),
            ImmutableList.of(new TypeParameter(elemType), new NumericParameter(ctx.dim.getText())));
    }

    private void validateVectorType(BaseDataType elemType, String dimension) {
        if (!"FLOAT".equalsIgnoreCase(elemType.getTypeName().getValue())) {
            throw new IllegalArgumentException("VECTOR only supports FLOAT element type");
        }
        if (!dimension.matches("[0-9]+")) {
            throw new IllegalArgumentException("VECTOR dimension must be a positive integer");
        }
        try {
            int value = Integer.parseInt(dimension);
            if (value < 1 || value > 4096) {
                throw new IllegalArgumentException("VECTOR dimension must be in range [1,4096]");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("VECTOR dimension must be a positive integer", e);
        }
    }

    @Override
    public Node visitListType(ListTypeContext ctx) {
        Node visit = visit(ctx.builtinTypeOrUdt().type());
        return new MaxComputeGenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            ctx.KW_ARRAY().getText(),
            ImmutableList.of(new TypeParameter((BaseDataType)visit)));
    }

    @Override
    public Node visitStructType(StructTypeContext ctx) {
        List<Field> list = visit(ctx.columnNameColonTypeList().columnNameColonType(), Field.class);
        return new RowDataType(getLocation(ctx), getOrigin(ctx), list);
    }

    @Override
    public Node visitColumnNameColonType(ColumnNameColonTypeContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        BaseDataType baseDataType = (BaseDataType)visit(ctx.t);
        StringLiteral stringLiteral = visitIfPresent(ctx.c, StringLiteral.class).orElse(null);
        Comment comment = null;
        if (stringLiteral != null) {
            comment = new Comment(stringLiteral.getValue());
        }
        return new Field(identifier, baseDataType, comment);
    }

    @Override
    public Node visitMapType(MapTypeContext ctx) {
        Node keyVisit = visit(ctx.primitiveTypeOrUdt());
        Node valueVisit = visit(ctx.builtinTypeOrUdt());
        return new GenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            ctx.KW_MAP().getText(),
            ImmutableList.of(new TypeParameter((BaseDataType)keyVisit), new TypeParameter((BaseDataType)valueVisit)));
    }

    @Override
    public Node visitNullableSpec(NullableSpecContext ctx) {
        if (ctx.KW_NOT() != null) {
            return new NotNullConstraint(IdentifierUtil.sysIdentifier());
        }
        return null;
    }

    @Override
    public Node visitKeyValueProperty(KeyValuePropertyContext ctx) {
        return new Property(
            StripUtils.strip(ctx.key.getText()),
            StripUtils.strip(ctx.value.getText())
        );
    }

    @Override
    public Node visitTableName(TableNameContext ctx) {
        return new Table(QualifiedName.of(visit(ctx.identifier(), Identifier.class)));
    }

    private <T> List<T> visit(List<? extends ParseTree> contexts, Class<T> clazz) {
        return ParserHelper.visit(this, contexts, clazz);
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
        return ParserHelper.visitIfPresent(this, context, clazz);
    }

    private void buildExternalProperties(CreateTableStatementContext ctx, List<Property> properties) {
        if (ctx.ext == null) {
            return;
        }
        properties.add(new Property(MaxComputePropertyKey.EXTERNAL.getValue(), new BooleanLiteral(BooleanLiteral.TRUE)));
        if (ctx.dd == null) {
            return;
        }
        OdpsParser.DataFormatDescContext dataFormatDescContext = ctx.dd;
        // location
        if (dataFormatDescContext.loc != null) {
            String location = StripUtils.strip(dataFormatDescContext.loc.locn.getText());
            properties.add(new Property(MaxComputePropertyKey.LOCATION.getValue(), new StringLiteral(location)));
        }
        // storage handler
        if (dataFormatDescContext.ff != null && dataFormatDescContext.ff.storageHandler != null) {
            String storageHandler = StripUtils.strip(dataFormatDescContext.ff.storageHandler.getText());
            properties.add(new Property(MaxComputePropertyKey.STORED_BY.getValue(), new StringLiteral(storageHandler)));
        }
        // serde props
        if (dataFormatDescContext.ff != null && dataFormatDescContext.ff.serdeProperties() != null) {
            List<KeyValuePropertyContext> keyValuePropertyContexts = dataFormatDescContext.ff.serdeProperties().tableProperties().tablePropertiesList().keyValueProperty();
            keyValuePropertyContexts.forEach(context -> {
                String propKey = StripUtils.strip(context.getChild(0).getText());
                String propValue = StripUtils.strip(context.getChild(2).getText());
                properties.add(new Property(StringUtils.join(Lists.newArrayList(MaxComputePropertyKey.SERDE_PROPS.getValue(), propKey),
                    "."), new StringLiteral(propValue)));
            });
        }
        if (dataFormatDescContext.rf != null && dataFormatDescContext.rf.rfs != null && dataFormatDescContext.rf.rfs.serdeProperties() != null) {
            List<KeyValuePropertyContext> keyValuePropertyContexts = dataFormatDescContext.rf.rfs.serdeProperties().tableProperties().tablePropertiesList().keyValueProperty();
            keyValuePropertyContexts.forEach(context -> {
                String propKey = StripUtils.strip(context.getChild(0).getText());
                String propValue = StripUtils.strip(context.getChild(2).getText());
                properties.add(new Property(StringUtils.join(Lists.newArrayList(MaxComputePropertyKey.SERDE_PROPS.getValue(), propKey),
                    "."), new StringLiteral(propValue)));
            });
        }
        // stored as
        if (dataFormatDescContext.ff != null && dataFormatDescContext.ff.genericSpec != null) {
            String genericSpec = StripUtils.strip(dataFormatDescContext.ff.genericSpec.getText());
            properties.add(new Property(MaxComputePropertyKey.STORED_AS.getValue(), new StringLiteral(genericSpec)));
        }
        if (dataFormatDescContext.ff != null && dataFormatDescContext.ff.inFmt != null) {
            String inFmt = StripUtils.strip(dataFormatDescContext.ff.inFmt.getText());
            properties.add(new Property(MaxComputePropertyKey.INPUT_FORMAT.getValue(), new StringLiteral(inFmt)));
        }
        if (dataFormatDescContext.ff != null && dataFormatDescContext.ff.outFmt != null) {
            String outFmt = StripUtils.strip(dataFormatDescContext.ff.outFmt.getText());
            properties.add(new Property(MaxComputePropertyKey.OUTPUT_FORMAT.getValue(), new StringLiteral(outFmt)));
        }

        // row format serde
        if (dataFormatDescContext.rf != null && dataFormatDescContext.rf.rfs != null) {
            String rowFormatSerde = StripUtils.strip(dataFormatDescContext.rf.rfs.name.getText());
            properties.add(new Property(MaxComputePropertyKey.ROW_FORMAT_SERDE.getValue(), new StringLiteral(rowFormatSerde)));
        }
        // using
        if (dataFormatDescContext.res != null && dataFormatDescContext.res.res != null) {
            String res = StripUtils.strip(dataFormatDescContext.res.res.getText());
            properties.add(new Property(MaxComputePropertyKey.USING.getValue(), new StringLiteral(res)));
        }
    }
}
