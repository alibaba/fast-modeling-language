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
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
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
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ColumnConstraintContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ColumnCreateTableContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ColumnDeclarationContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ColumnDefinitionContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.CommentColumnConstraintContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ConstantContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.CreateDefinitionsContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.DecimalLiteralContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.FullColumnNameContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.FunctionNameBaseContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.LifeCycleContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.PartitionFunctionDefinitionContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.PartitionValueContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.PrimaryKeyColumnConstraintContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.RootContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.ScalarFunctionCallContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.SimpleDataTypeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.SqlStatementsContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.StoragePolicyContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.StoragePolicyValueContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.StringLiteralContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableAttributeContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableNameContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableOptionCommentContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.TableOptionContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.UidContext;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlParser.UniqueKeyColumnConstraintContext;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
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

    public static final String COMMENT = "comment";
    public static final String DATE_FORMAT = "DATE_FORMAT";

    private final ReverseContext reverseContext;

    public AdbMysqlAstBuilder(ReverseContext context) {
        this.reverseContext = context == null ? ReverseContext.builder().build() : context;
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
        if (visit.size() == 0) {
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
        List<BaseConstraint> constraints = ImmutableList.of();
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
            Property property = (Property)visit(ctx.tableAttribute());
            otherProperty.add(property);
        }

        if (ctx.blockSize() != null) {
            Property property = (Property)visit(ctx.blockSize());
            otherProperty.add(property);
        }

        if (ctx.storagePolicy() != null) {
            List<Property> storagePolicy = process(ctx.storagePolicy());
            otherProperty.addAll(storagePolicy);
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

    private List<Property> process(StoragePolicyContext storagePolicy) {
        StoragePolicyValueContext storagePolicyValueContext = storagePolicy.storagePolicyValue();
        TerminalNode terminalNode = storagePolicyValueContext.STRING_LITERAL();
        Property property = new Property(AdbMysqlPropertyKey.STORAGE_POLICY.getValue(), StripUtils.strip(terminalNode.getText()));
        if (storagePolicyValueContext.HOT_PARTITION_COUNT() != null) {
            Property p2 = new Property(AdbMysqlPropertyKey.HOT_PARTITION_COUNT.getValue(),
                storagePolicyValueContext.decimalLiteral().getText());
            return Lists.newArrayList(property, p2);
        }
        return Lists.newArrayList(property);
    }

    @Override
    public Node visitPartitionValue(PartitionValueContext ctx) {
        if (ctx.uid() != null) {
            Identifier identifier = (Identifier)visit(ctx.uid());
            return new PartitionedBy(Lists.newArrayList(ColumnDefinition.builder().colName(identifier).build()));
        } else if (ctx.expression() != null) {
            FunctionCall functionCall = (FunctionCall)visit(ctx.expression());
            if (functionCall != null && StringUtils.equalsIgnoreCase(functionCall.getFuncName().toString(), DATE_FORMAT)) {
                List<BaseExpression> arguments = functionCall.getArguments();
                TableOrColumn baseExpression = (TableOrColumn)arguments.get(0);
                return new PartitionedBy(
                    Lists.newArrayList(ColumnDefinition.builder().colName(new Identifier(baseExpression.getQualifiedName().getSuffix())).build()));
            }
        }
        return null;
    }

    @Override
    public Node visitScalarFunctionCall(ScalarFunctionCallContext ctx) {
        Identifier visit = (Identifier)visit(ctx.scalarFunctionName().functionNameBase());
        int childCount = ctx.functionArgs().getChildCount();
        List<BaseExpression> list = Lists.newArrayList();
        for (int i = 0; i < childCount; i++) {
            ParseTree child = ctx.functionArgs().getChild(i);
            BaseExpression visit1 = (BaseExpression)visit(child);
            list.add(visit1);
        }
        return new FunctionCall(QualifiedName.of(Lists.newArrayList(visit)), false, list);
    }

    @Override
    public Node visitFullColumnName(FullColumnNameContext ctx) {
        return new TableOrColumn(QualifiedName.of(ctx.getText()));
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
            return new Property(AdbMysqlPropertyKey.DISTRIBUTED_BY.getValue(), "BROADCAST");
        } else {
            List<Identifier> visit = ParserHelper.visit(this, ctx.colNames().uid(), Identifier.class);
            return new Property(AdbMysqlPropertyKey.DISTRIBUTED_BY.getValue(),
                visit.stream().map(Identifier::getValue).collect(Collectors.joining(","))
            );
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
    public Node visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraintContext ctx) {
        return new PrimaryConstraint(IdentifierUtil.sysIdentifier(), ImmutableList.of(), true);
    }

    @Override
    public Node visitUniqueKeyColumnConstraint(UniqueKeyColumnConstraintContext ctx) {
        return new UniqueConstraint(IdentifierUtil.sysIdentifier(), ImmutableList.of(), true);
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
