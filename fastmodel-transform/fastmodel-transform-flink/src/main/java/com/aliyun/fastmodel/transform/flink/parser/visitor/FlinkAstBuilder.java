package com.aliyun.fastmodel.transform.flink.parser.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.WaterMarkConstraint;
import com.aliyun.fastmodel.transform.flink.format.FlinkColumnPropertyKey;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.ColumnTypeContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.CommentSpecContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.ComputedColumnDefinitionContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.ComputedColumnExpressionContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.DecimalLiteralContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.ExpressionContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.MetadataColumnDefinitionContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.NonReservedKeywordsAlternativeContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.PhysicalColumnDefinitionContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.QuotedIdentifierContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.SimpleCreateTableContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.SingleStatementContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.SourceTableContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.SqlStatementsContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.StringLiteralContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.TableConstraintContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.TablePropertyContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.UnquotedIdentifierContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser.WatermarkDefinitionContext;
import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParserBaseVisitor;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkDataTypeName;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkGenericDataType;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkRawDataType;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkRowDataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.Token;
import org.apache.commons.collections.CollectionUtils;

/**
 * @author 子梁
 * @date 2024/5/15
 */
public class FlinkAstBuilder extends FlinkSqlParserBaseVisitor<Node> {

    private ReverseContext flinkTransformContext;

    public FlinkAstBuilder(ReverseContext context) {
        this.flinkTransformContext = context;
    }

    @Override
    public Node visitSqlStatements(SqlStatementsContext ctx) {
        List<BaseStatement> list = ParserHelper.visit(this, ctx.singleStatement(), BaseStatement.class);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        if (list.size() == 1) {
            return list.get(0);
        }
        return new CompositeStatement(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), list);
    }

    @Override
    public Node visitSingleStatement(SingleStatementContext ctx) {
        return visit(ctx.sqlStatement());
    }

    @Override
    public Node visitSimpleCreateTable(SimpleCreateTableContext ctx) {
        boolean ifNotExists = ctx.ifNotExists() != null;

        QualifiedName tableName = (QualifiedName) visit(ctx.sourceTable());
        Comment comment = processComment(ctx.commentSpec());

        List<ColumnDefinition> columns = ParserHelper.visit(this, ctx.columnOptionDefinition(),
            ColumnDefinition.class);

        List<BaseConstraint> constraints = new ArrayList<>();
        if (ctx.tableConstraint() != null) {
            PrimaryConstraint primaryConstraint = (PrimaryConstraint) visit(ctx.tableConstraint());
            constraints.add(primaryConstraint);
        }
        if (ctx.watermarkDefinition() != null) {
            WaterMarkConstraint waterMarkConstraint = (WaterMarkConstraint) visit(ctx.watermarkDefinition());
            constraints.add(waterMarkConstraint);
        }

        PartitionedBy partition = null;
        if (ctx.partitionDefinition() != null) {
            List<Identifier> columnNames = ParserHelper.visit(this, ctx.partitionDefinition().transformList().transform(), Identifier.class);
            List<ColumnDefinition> partitionColumns = columnNames.stream()
                .map(columnName -> ColumnDefinition.builder().colName(columnName).build())
                .collect(Collectors.toList());
            partition = new PartitionedBy(partitionColumns);
        }

        List<Property> properties = null;
        if (ctx.withOption() != null) {
            properties = ParserHelper.visit(this, ctx.withOption().tablePropertyList().tableProperty(), Property.class);
        }

        return CreateTable.builder()
            .ifNotExist(ifNotExists)
            .tableName(tableName)
            .columns(columns)
            .partition(partition)
            .constraints(constraints)
            .properties(properties)
            .comment(comment)
            .build();
    }

    @Override
    public QualifiedName visitSourceTable(SourceTableContext ctx) {
        List<Identifier> visit = ParserHelper.visit(this, ctx.uid().identifier(), Identifier.class);
        return QualifiedName.of(visit);
    }

    @Override
    public ColumnDefinition visitPhysicalColumnDefinition(PhysicalColumnDefinitionContext ctx) {
        Identifier columnName = (Identifier) visit(ctx.columnName());
        BaseDataType dataType = (BaseDataType) visit(ctx.columnType());
        Comment comment = processComment(ctx.commentSpec());

        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName(columnName)
            .dataType(dataType)
            .comment(comment)
            .build();

        if (ctx.columnConstraint() != null && ctx.columnConstraint().KW_PRIMARY() != null) {
            columnDefinition.setPrimary(true);
        }
        if (ctx.columnConstraint() != null && ctx.columnConstraint().KW_NOT() != null && ctx.columnConstraint().KW_NULL() != null) {
            columnDefinition.setNotNull(true);
        }

        return columnDefinition;
    }

    @Override
    public BaseDataType visitColumnType(ColumnTypeContext ctx) {
        if (ctx.KW_ARRAY() != null) {
            return new FlinkGenericDataType(
                getLocation(ctx),
                getOrigin(ctx),
                ctx.KW_ARRAY().getText(),
                ImmutableList.of(new TypeParameter((BaseDataType) visit(ctx.lengthOneTypeDimension().columnType()))));
        } else if (ctx.KW_MAP() != null) {
            return new FlinkGenericDataType(
                getLocation(ctx),
                getOrigin(ctx),
                ctx.KW_MAP().getText(),
                ImmutableList.of(
                    new TypeParameter((BaseDataType) visit(ctx.mapTypeDimension().columnType(0))),
                    new TypeParameter((BaseDataType) visit(ctx.mapTypeDimension().columnType(1)))));
        } else if (ctx.KW_ROW() != null) {
            List<Identifier> columnNames = ParserHelper.visit(this, ctx.rowTypeDimension().columnName(), Identifier.class);
            List<BaseDataType> dataTypes = ParserHelper.visit(this, ctx.rowTypeDimension().columnType(), BaseDataType.class);
            List<Field> fields = new ArrayList<>();
            for (int i = 0; i < columnNames.size(); i++) {
                Identifier columnName = columnNames.get(i);
                BaseDataType dataType = dataTypes.get(i);
                fields.add(new Field(columnName, dataType, null));
            }
            return new FlinkRowDataType(fields);
        } else if (ctx.KW_RAW() != null) {
            List<StringLiteral> columnNames = ParserHelper.visit(this, ctx.lengthTwoStringDimension().stringLiteral(), StringLiteral.class);
            List<QualifiedName> fields = columnNames.stream()
                .map(columnName -> QualifiedName.of(columnName.getValue()))
                .collect(Collectors.toList());
            return new FlinkRawDataType(fields);
        } else {
            Token name = ctx.typeName;
            IDataTypeName byValue = FlinkDataTypeName.getByValue(name.getText());
            List<DataTypeParameter> list = Lists.newArrayList();
            if (ctx.lengthOneDimension() != null) {
                NumericParameter numericParameter = (NumericParameter) visit(ctx.lengthOneDimension().decimalLiteral());
                list.add(numericParameter);
            }
            if (ctx.lengthTwoOptionalDimension() != null) {
                List<NumericParameter> numericParameters = ParserHelper.visit(this,
                    ctx.lengthTwoOptionalDimension().decimalLiteral(), NumericParameter.class);
                list.addAll(numericParameters);
            }
            return new FlinkGenericDataType(getLocation(ctx), getOrigin(ctx), byValue.getValue(), list);
        }
    }

    @Override
    public ColumnDefinition visitMetadataColumnDefinition(MetadataColumnDefinitionContext ctx) {
        Identifier columnName = (Identifier) visit(ctx.columnName());
        BaseDataType dataType = (BaseDataType) visit(ctx.columnType());

        List<Property> columnProperties = new ArrayList<>();
        columnProperties.add(new Property(FlinkColumnPropertyKey.METADATA.getValue(), new BooleanLiteral("true")));
        if (ctx.metadataKey() != null) {
            columnProperties.add(new Property(FlinkColumnPropertyKey.METADATA_KEY.getValue(), StripUtils.strip(ctx.metadataKey().getText())));
        }
        if (ctx.KW_VIRTUAL() != null) {
            columnProperties.add(new Property(FlinkColumnPropertyKey.VIRTUAL.getValue(), new BooleanLiteral("true")));
        }

        return ColumnDefinition.builder()
            .colName(columnName)
            .dataType(dataType)
            .properties(columnProperties)
            .build();
    }

    @Override
    public ColumnDefinition visitComputedColumnDefinition(ComputedColumnDefinitionContext ctx) {
        Identifier columnName = (Identifier) visit(ctx.columnName());
        Comment comment = processComment(ctx.commentSpec());

        List<Property> columnProperties = new ArrayList<>();
        columnProperties.add(new Property(FlinkColumnPropertyKey.COMPUTED.getValue(), new BooleanLiteral("true")));
        if (ctx.computedColumnExpression() != null) {
            ComputedColumnExpressionContext computedColumnExpressionContext = ctx.computedColumnExpression();
            columnProperties.add(new Property(FlinkColumnPropertyKey.COMPUTED_COLUMN_EXPRESSION.getValue(),
                new StringLiteral(getLocation(computedColumnExpressionContext),
                    getOrigin(computedColumnExpressionContext),
                    StripUtils.strip(computedColumnExpressionContext.getText()))));
        }

        return ColumnDefinition.builder()
            .colName(columnName)
            .comment(comment)
            .properties(columnProperties)
            .build();
    }

    @Override
    public PrimaryConstraint visitTableConstraint(TableConstraintContext ctx) {
        Identifier constraintName = IdentifierUtil.sysIdentifier();
        if (ctx.constraintName() != null) {
            constraintName = new Identifier(ctx.constraintName().getText());
        }

        List<Identifier> columnNameList = ParserHelper.visit(this, ctx.columnNameList().columnName(), Identifier.class);

        return new PrimaryConstraint(constraintName, columnNameList);
    }

    @Override
    public WaterMarkConstraint visitWatermarkDefinition(WatermarkDefinitionContext ctx) {
        Identifier columnName = (Identifier) visit(ctx.expression(0));
        ExpressionContext expression = ctx.expression(1);
        return new WaterMarkConstraint(IdentifierUtil.sysIdentifier(), columnName,
            new StringLiteral(getLocation(expression), getOrigin(expression), expression.getText()));
    }

    @Override
    public Property visitTableProperty(TablePropertyContext ctx) {
        Identifier propertyKey = (Identifier) visit(ctx.tablePropertyKey());
        String propertyValue = StripUtils.strip(ctx.tablePropertyValue().getText());

        return new Property(propertyKey.getValue(), propertyValue);
    }

    @Override
    public Identifier visitQuotedIdentifier(QuotedIdentifierContext ctx) {
        return new Identifier(getLocation(ctx), getOrigin(ctx), StripUtils.strip(ctx.getText()));
    }

    @Override
    public Identifier visitNonReservedKeywordsAlternative(NonReservedKeywordsAlternativeContext ctx) {
        return new Identifier(getLocation(ctx), getOrigin(ctx), StripUtils.strip(ctx.getText()));
    }

    @Override
    public Identifier visitUnquotedIdentifier(UnquotedIdentifierContext ctx) {
        return new Identifier(getLocation(ctx), getOrigin(ctx), StripUtils.strip(ctx.getText()));
    }

    @Override
    public NumericParameter visitDecimalLiteral(DecimalLiteralContext ctx) {
        return new NumericParameter(ctx.DIG_LITERAL().getText());
    }

    @Override
    public Comment visitCommentSpec(CommentSpecContext ctx) {
        if (ctx == null) {
            return new Comment(null);
        }
        return new Comment(StripUtils.strip(ctx.getText()));
    }

    @Override
    public StringLiteral visitStringLiteral(StringLiteralContext ctx) {
        return new StringLiteral(StripUtils.strip(ctx.getText()));
    }

    private Comment processComment(CommentSpecContext ctx) {
        if (ctx == null) {
            return null;
        }

        return new Comment(StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

}
