package com.aliyun.fastmodel.transform.adbmysql.parser;

import java.util.List;
import java.util.stream.Collectors;

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
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.adbmysql.format.AdbMysqlPropertyKey;
import com.aliyun.fastmodel.transform.adbmysql.format.AdbMysqlTimeFunctionType;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ColumnConstraintContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ColumnCreateTableContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ColumnDeclarationContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ColumnDefinitionContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.CommentColumnConstraintContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ConstantContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.CreateDefinitionsContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.DecimalLiteralContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.DimensionDataTypeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.FullColumnNameContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.FunctionNameBaseContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.IndexColumnNameContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.LengthOneDimensionContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.LifeCycleContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ListDataTypeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.LongVarcharDataTypeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.MapDataTypeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.NationalStringDataTypeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.NationalVaryingStringDataTypeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.NullColumnConstraintContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.PartitionFunctionDefinitionContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.PartitionValueContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.PrimaryKeyColumnConstraintContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.PrimaryKeyTableConstraintContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.RootContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ScalarFunctionCallContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.SimpleDataTypeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.SpatialDataTypeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.SqlStatementsContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.StringDataTypeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.StringLiteralContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableAttributeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableNameContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableOpitonStoragePolicyContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableOptionBlockSizeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableOptionCommentContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableOptionContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableOptionEngineContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableOptionHotPartitionCountContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableOptionIndexAllContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableOptionTablePropertiesContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.UidContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.UniqueKeyColumnConstraintContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.UniqueKeyTableConstraintContext;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ExpressionPartitionBy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.Token;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getIdentifier;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.visitIfPresent;

/**
 * AdbMysqlAstBuilder
 *
 * @author panguanjing
 * @date 2023/2/10
 */
public class AdbMysqlAstBuilder extends AdbMysqlParserBaseVisitor<Node> {

    public static final String COMMENT = "COMMENT";

    private final ReverseContext reverseContext;

    private final AdbMysqlLanguageParser adbMysqlLanguageParser;

    public AdbMysqlAstBuilder(ReverseContext context) {
        this.reverseContext = context == null ? ReverseContext.builder().build() : context;
        this.adbMysqlLanguageParser = new AdbMysqlLanguageParser();
    }

    @Override
    public Node visitRoot(RootContext ctx) {
        SqlStatementsContext tree = ctx.sqlStatements();
        if (tree == null) {
            throw new ParseException("statement is invalid");
        }
        return visit(tree);
    }

    @Override
    public Node visitSqlStatements(SqlStatementsContext ctx) {
        List<BaseStatement> visit = ParserHelper.visit(this, ctx.sqlStatement(), BaseStatement.class);
        if (visit.isEmpty()) {
            throw new ParseException("parse error with text:" + ParserHelper.getOrigin(ctx));
        }
        if (visit.size() > 1) {
            return new CompositeStatement(visit);
        } else {
            return visit.get(0);
        }
    }

    @Override
    public Node visitColumnCreateTable(ColumnCreateTableContext ctx) {
        List<Property> properties = ImmutableList.of();
        List<TableOptionContext> tableOptionContexts = ctx.tableOption();
        if (CollectionUtils.isNotEmpty(tableOptionContexts)) {
            properties = ParserHelper.visit(this, tableOptionContexts, Property.class);
        }
        List<ColumnDefinition> columnDefinitions = ImmutableList.of();
        List<TableIndex> tableIndexList = ImmutableList.of();
        List<BaseConstraint> constraints = Lists.newArrayList();
        CreateDefinitionsContext definitions = ctx.createDefinitions();
        if (definitions != null) {
            List<Node> nodes = ParserHelper.visit(this, definitions.createDefinition(), Node.class);
            columnDefinitions = nodes.stream().filter(x -> x instanceof ColumnDefinition).map(x -> (ColumnDefinition)x).collect(Collectors.toList());
            constraints = nodes.stream().filter(x -> x instanceof BaseConstraint).map(x -> (BaseConstraint)x).collect(Collectors.toList());

            tableIndexList = nodes.stream().filter(x -> x instanceof TableIndex).map(x -> (TableIndex)x).collect(Collectors.toList());
        }
        PartitionedBy partitionedBy = null;
        if (ctx.partitionDefinitions() != null) {
            PartitionFunctionDefinitionContext partitionFunctionDefinitionContext = ctx.partitionDefinitions().partitionFunctionDefinition();
            partitionedBy = visitIfPresent(this, partitionFunctionDefinitionContext, PartitionedBy.class).orElse(null);
        }

        Property commentProperties = getCommentProperty(properties);

        List<Property> otherProperty = properties.stream().filter(p -> !p.getName().equalsIgnoreCase(COMMENT)).collect(Collectors.toList());

        if (partitionedBy != null) {
            Property property = visitIfPresent(this, ctx.partitionDefinitions().lifeCycle(), Property.class).orElse(null);
            if (property != null) {
                otherProperty.add(property);
            }
        }

        if (ctx.tableAttribute() != null) {
            BaseConstraint constraint = (BaseConstraint)visit(ctx.tableAttribute());
            constraints.add(constraint);
        }

        Comment comment = commentProperties != null ? new Comment(commentProperties.getValue())
            : null;
        return CreateTable.builder()
            .tableName(getQualifiedName(ctx.tableName()))
            .detailType(reverseContext.getReverseTableType())
            .columns(columnDefinitions)
            .constraints(constraints)
            .properties(otherProperty)
            .partition(partitionedBy)
            .comment(comment)
            .tableIndex(tableIndexList)
            .ifNotExist(ctx.ifNotExists() != null)
            .build();
    }

    @Override
    public Node visitTableOpitonStoragePolicy(TableOpitonStoragePolicyContext ctx) {
        return new Property(AdbMysqlPropertyKey.STORAGE_POLICY.getValue(), StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public Node visitTableOptionIndexAll(TableOptionIndexAllContext ctx) {
        return new Property(AdbMysqlPropertyKey.INDEX_ALL.getValue(), StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public Node visitTableOptionEngine(TableOptionEngineContext ctx) {
        return new Property(AdbMysqlPropertyKey.ENGINE.getValue(), StripUtils.strip(ctx.engineName().getText()));
    }

    @Override
    public Node visitTableOptionTableProperties(TableOptionTablePropertiesContext ctx) {
        return new Property(AdbMysqlPropertyKey.TABLE_PROPERTIES.getValue(), StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public Node visitTableOptionBlockSize(TableOptionBlockSizeContext ctx) {
        return new Property(AdbMysqlPropertyKey.BLOCK_SIZE.getValue(), ctx.decimalLiteral().getText());
    }

    @Override
    public Node visitTableOptionHotPartitionCount(TableOptionHotPartitionCountContext ctx) {
        return new Property(AdbMysqlPropertyKey.HOT_PARTITION_COUNT.getValue(), ctx.decimalLiteral().getText());
    }

    @Override
    public Node visitPartitionValue(PartitionValueContext ctx) {
        if (ctx.uid() != null) {
            Identifier identifier = (Identifier)visit(ctx.uid());
            //因为adb mysql会在data_format函数外增加``, 导致语法解析为了uid，这里需要兼容下，如果identifier中包含函数
            String value = identifier.getValue();
            BaseExpression baseExpression = adbMysqlLanguageParser.parseExpression(value);
            if (baseExpression instanceof FunctionCall) {
                return getExpressionPartitionBy((FunctionCall)baseExpression);
            } else {
                return new PartitionedBy(Lists.newArrayList(ColumnDefinition.builder().colName(identifier).build()));
            }
        } else if (ctx.expression() != null) {
            FunctionCall functionCall = (FunctionCall)visit(ctx.expression());
            return getExpressionPartitionBy(functionCall);
        }
        return null;
    }

    private static ExpressionPartitionBy getExpressionPartitionBy(FunctionCall functionCall) {
        AdbMysqlTimeFunctionType adbMysqlTimeFunctionType = AdbMysqlTimeFunctionType.getByValue(
            functionCall.getFuncName().getSuffix()
        );
        List<ColumnDefinition> list = Lists.newArrayList();
        if (adbMysqlTimeFunctionType != null) {
            TableOrColumn tableOrColumn = (TableOrColumn)functionCall.getArguments().get(0);
            Identifier identifier = new Identifier(tableOrColumn.getQualifiedName().getSuffix());
            list.add(ColumnDefinition.builder().colName(identifier).build());
        }
        return new ExpressionPartitionBy(
            list,
            functionCall,
            null
        );
    }

    @Override
    public Node visitScalarFunctionCall(ScalarFunctionCallContext ctx) {
        Identifier visit = (Identifier)visit(ctx.scalarFunctionName().functionNameBase());
        List<BaseExpression> list = Lists.newArrayList();
        if (ctx.functionArgs() != null) {
            list = ParserHelper.visit(this, ctx.functionArgs().functionArg(), BaseExpression.class);
        }
        return new FunctionCall(QualifiedName.of(Lists.newArrayList(visit)), false, list);
    }

    @Override
    public Node visitFullColumnName(FullColumnNameContext ctx) {
        return new TableOrColumn(QualifiedName.of(ctx.getText()));
    }

    @Override
    public Node visitIndexColumnName(IndexColumnNameContext ctx) {
        return visit(ctx.uid());
    }

    @Override
    public Node visitConstant(ConstantContext ctx) {
        return super.visitConstant(ctx);
    }

    @Override
    public Node visitDecimalLiteral(DecimalLiteralContext ctx) {
        return new DecimalLiteral(ctx.getText());
    }

    @Override
    public Node visitStringLiteral(StringLiteralContext ctx) {
        return new StringLiteral(StripUtils.strip(ctx.getText()));
    }

    @Override
    public Node visitFunctionNameBase(FunctionNameBaseContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitLifeCycle(LifeCycleContext ctx) {
        return new Property(AdbMysqlPropertyKey.LIFE_CYCLE.getValue(), ctx.decimalLiteral().getText());
    }

    @Override
    public Node visitColumnDeclaration(ColumnDeclarationContext ctx) {
        UidContext uid = ctx.fullColumnName().uid();
        Identifier identifier = getIdentifier(uid);
        ColumnDefinitionContext columnDefinitionContext = ctx.columnDefinition();
        List<ColumnConstraintContext> columnConstraintContexts = columnDefinitionContext.columnConstraint();
        Comment c = null;
        Boolean primary = null;
        Boolean notNull = null;
        BaseLiteral defaultValue = null;
        if (columnConstraintContexts != null) {
            List<Node> list = ParserHelper.visit(this, columnConstraintContexts, Node.class);
            c = list.stream().filter(x -> x instanceof Comment).map(x -> (Comment)x).findFirst().orElse(null);
            PrimaryConstraint primaryConstraint = list.stream().filter(constraint -> constraint instanceof PrimaryConstraint).map(
                    constraint -> (PrimaryConstraint)constraint).findFirst()
                .orElse(null);
            if (primaryConstraint != null) {
                primary = primaryConstraint.getEnable();
            }
            NotNullConstraint notNullConstraint = list.stream().filter(x -> x instanceof NotNullConstraint).map(x -> (NotNullConstraint)x).findFirst()
                .orElse(null);
            if (notNullConstraint != null) {
                notNull = notNullConstraint.getEnable();
            }
        }
        BaseDataType baseDataType = (BaseDataType)visit(columnDefinitionContext.dataType());
        return ColumnDefinition.builder()
            .colName(identifier)
            .primary(primary)
            .notNull(notNull)
            .dataType(baseDataType)
            .comment(c)
            .defaultValue(defaultValue)
            .build();
    }

    @Override
    public Node visitTableAttribute(TableAttributeContext ctx) {
        if (ctx.BROADCAST() != null) {
            return new DistributeConstraint(null, true, null);
        } else {
            List<Identifier> visit = ParserHelper.visit(this, ctx.colNames().uid(), Identifier.class);
            return new DistributeConstraint(visit, null);
        }
    }

    @Override
    public Node visitTableOptionComment(TableOptionCommentContext ctx) {
        return new Property(COMMENT, StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public Node visitSimpleDataType(SimpleDataTypeContext ctx) {
        return new GenericDataType(new Identifier(ctx.typeName.getText()), ImmutableList.of());
    }

    @Override
    public Node visitStringDataType(StringDataTypeContext ctx) {
        Token typeName = ctx.typeName;
        List<DataTypeParameter> dataTypeParameterList = null;
        if (ctx.lengthOneDimension() != null) {
            dataTypeParameterList = Lists.newArrayList((DataTypeParameter)visit(ctx.lengthOneDimension()));
        }
        return new GenericDataType(new Identifier(typeName.getText()), dataTypeParameterList);
    }

    @Override
    public Node visitLengthOneDimension(LengthOneDimensionContext ctx) {
        DecimalLiteral visit = (DecimalLiteral)visit(ctx.decimalLiteral());
        return new NumericParameter(visit.getNumber());
    }

    @Override
    public Node visitNationalVaryingStringDataType(NationalVaryingStringDataTypeContext ctx) {
        return super.visitNationalVaryingStringDataType(ctx);
    }

    @Override
    public Node visitNationalStringDataType(NationalStringDataTypeContext ctx) {
        return super.visitNationalStringDataType(ctx);
    }

    @Override
    public Node visitDimensionDataType(DimensionDataTypeContext ctx) {
        Token typeName = ctx.typeName;
        List<DataTypeParameter> dataTypeParameterList = null;
        if (ctx.lengthOneDimension() != null) {
            dataTypeParameterList = Lists.newArrayList((DataTypeParameter)visit(ctx.lengthOneDimension()));
        } else if (ctx.lengthTwoDimension() != null) {
            dataTypeParameterList = ParserHelper.visit(this, ctx.lengthTwoDimension().decimalLiteral(), DecimalLiteral.class).stream()
                .map(d -> new NumericParameter(d.getNumber())).collect(Collectors.toList());
        }
        return new GenericDataType(new Identifier(typeName.getText()), dataTypeParameterList);
    }

    @Override
    public Node visitSpatialDataType(SpatialDataTypeContext ctx) {
        return new GenericDataType(new Identifier(ctx.typeName.getText()));
    }

    @Override
    public Node visitListDataType(ListDataTypeContext ctx) {
        BaseDataType type = (BaseDataType)visit(ctx.dataType());
        TypeParameter typeParameter = new TypeParameter(type);
        List<DataTypeParameter> dataTypeList = Lists.newArrayList(typeParameter);
        return new GenericDataType(ctx.typeName.getText(), dataTypeList);
    }

    @Override
    public Node visitMapDataType(MapDataTypeContext ctx) {
        List<DataTypeParameter> dataTypeList =
            ParserHelper.visit(this, ctx.dataType(), BaseDataType.class)
                .stream().map(
                    TypeParameter::new
                ).collect(Collectors.toList());
        return new GenericDataType(ctx.typeName.getText(), dataTypeList);
    }

    @Override
    public Node visitLongVarcharDataType(LongVarcharDataTypeContext ctx) {
        return new GenericDataType(ctx.typeName.getText());
    }

    @Override
    public Node visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraintContext ctx) {
        return new PrimaryConstraint(IdentifierUtil.sysIdentifier(), ImmutableList.of(), true);
    }

    @Override
    public Node visitUniqueKeyColumnConstraint(UniqueKeyColumnConstraintContext ctx) {
        return new UniqueConstraint(IdentifierUtil.sysIdentifier(), ImmutableList.of(), true);
    }

    @Override
    public Node visitNullColumnConstraint(NullColumnConstraintContext ctx) {
        if (ctx.nullNotnull().NOT() != null && ctx.nullNotnull().NULL_LITERAL() != null) {
            return new NotNullConstraint(IdentifierUtil.sysIdentifier(), true);
        }
        return new NotNullConstraint(IdentifierUtil.sysIdentifier(), false);
    }

    @Override
    public Node visitPrimaryKeyTableConstraint(PrimaryKeyTableConstraintContext ctx) {
        Identifier constraintName = null;
        if (ctx.name != null) {
            constraintName = (Identifier)visit(ctx.name);
        } else {
            constraintName = IdentifierUtil.sysIdentifier();
        }
        List<Identifier> colNames = ParserHelper.visit(this, ctx.indexColumnNames().indexColumnName(), Identifier.class);
        return new PrimaryConstraint(constraintName, colNames);
    }

    @Override
    public Node visitUniqueKeyTableConstraint(UniqueKeyTableConstraintContext ctx) {
        Identifier constraintName = null;
        if (ctx.name != null) {
            constraintName = (Identifier)visit(ctx.name);
        } else {
            constraintName = IdentifierUtil.sysIdentifier();
        }
        List<Identifier> colNames = ParserHelper.visit(this, ctx.indexColumnNames().indexColumnName(), Identifier.class);
        return new UniqueConstraint(constraintName, colNames);
    }

    @Override
    public Node visitCommentColumnConstraint(CommentColumnConstraintContext ctx) {
        String comment = ctx.STRING_LITERAL().getText();
        return new Comment(getLocation(ctx), StripUtils.strip(comment));
    }

    private Property getCommentProperty(List<Property> properties) {
        if (CollectionUtils.isEmpty(properties)) {
            return null;
        }
        return properties.stream()
            .filter(property -> StringUtils.equalsIgnoreCase(property.getName(), COMMENT))
            .findFirst()
            .orElse(null);
    }

    private QualifiedName getQualifiedName(TableNameContext tableName) {
        List<Identifier> list = ParserHelper.visit(this, tableName.fullId().uid(), Identifier.class);
        return QualifiedName.of(list);
    }

    @Override
    public Node visitUid(AdbMysqlParser.UidContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

}
