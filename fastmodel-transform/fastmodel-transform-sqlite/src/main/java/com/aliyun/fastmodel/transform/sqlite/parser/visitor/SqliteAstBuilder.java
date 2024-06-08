package com.aliyun.fastmodel.transform.sqlite.parser.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateOdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.Any_nameContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.Column_constraintContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.Column_defContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.Create_table_stmtContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.Foreign_key_clauseContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.Foreign_tableContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.NameContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.ParseContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.Sql_stmt_listContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.Table_constraintContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParser.Type_nameContext;
import com.aliyun.fastmodel.transform.sqlite.parser.SQLiteParserBaseVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.BooleanUtils;

/**
 * SqliteAstBuilder
 *
 * @author panguanjing
 * @date 2023/8/28
 */
public class SqliteAstBuilder extends SQLiteParserBaseVisitor<Node> {

    private final ReverseContext context;

    public SqliteAstBuilder(ReverseContext context) {
        this.context = context == null ? ReverseContext.builder().build() : context;
    }

    @Override
    public Node visitParse(ParseContext ctx) {
        List<Sql_stmt_listContext> sql_stmt_listContexts = ctx.sql_stmt_list();
        List<BaseStatement> visit = ParserHelper.visit(this, sql_stmt_listContexts, BaseStatement.class);
        if (sql_stmt_listContexts.size() == 1) {
            return visit(sql_stmt_listContexts.get(0));
        }
        return new CompositeStatement(visit);
    }

    @Override
    public Node visitSql_stmt_list(Sql_stmt_listContext ctx) {
        List<BaseStatement> visit = ParserHelper.visit(this, ctx.sql_stmt(), BaseStatement.class);
        if (visit.size() == 0) {
            throw new ParseException("parse error with text:" + ParserHelper.getOrigin(ctx));
        }
        if (visit.size() == 1) {
            return visit.get(0);
        }
        return new CompositeStatement(visit);
    }

    @Override
    public Node visitCreate_table_stmt(Create_table_stmtContext ctx) {
        //由用户指定路由的context的key内容

        Optional<Identifier> schemaName = ParserHelper.visitIfPresent(this, ctx.schema_name(), Identifier.class);
        Optional<Identifier> identifier = ParserHelper.visitIfPresent(this, ctx.table_name(), Identifier.class);
        QualifiedName t = null;
        if (schemaName.isPresent()) {
            t = QualifiedName.of(Lists.newArrayList(schemaName.get(), identifier.get()));
        } else {
            t = QualifiedName.of(Lists.newArrayList(identifier.get()));
        }
        List<BaseConstraint> constraints = new ArrayList<>();
        //all columns
        List<ColumnDefinition> columnDefinitions = ImmutableList.of();
        if (ctx.column_def() != null) {
            columnDefinitions = ParserHelper.visit(this, ctx.column_def(), ColumnDefinition.class);
        }
        //if primary constraint not exist
        if (ctx.table_constraint() != null) {
            constraints = ParserHelper.visit(this, ctx.table_constraint(), BaseConstraint.class);
        }
        boolean ifNotExist = ctx.IF_() != null && ctx.NOT_() != null;
        TableBuilder tableBuilder = null;
        TableDetailType tableDetailType = context.getReverseTableType();
        if (tableDetailType == null) {
            tableBuilder = CreateTable.builder();
        } else {
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
        }
        return tableBuilder
            .tableName(t)
            .detailType(tableDetailType)
            .ifNotExist(ifNotExist)
            .columns(columnDefinitions)
            .constraints(constraints)
            .build();
    }

    @Override
    public Node visitColumn_def(Column_defContext ctx) {
        Identifier colName = (Identifier)visit(ctx.column_name());
        BaseDataType baseDataType = null;
        if (ctx.type_name() != null) {
            baseDataType = (BaseDataType)visit(ctx.type_name());
        }
        List<BaseConstraint> baseConstraints = null;
        if (ctx.column_constraint() != null) {
            baseConstraints = ParserHelper.visit(this, ctx.column_constraint(), BaseConstraint.class);
        }
        Boolean primary = isPrimary(baseConstraints);
        return ColumnDefinition.builder()
            .colName(colName)
            .dataType(baseDataType)
            .primary(primary)
            .notNull(isNotNull(primary, baseConstraints))
            .build();
    }

    private Boolean isNotNull(Boolean primary, List<BaseConstraint> baseConstraints) {
        if (BooleanUtils.isTrue(primary)) {
            return true;
        }
        boolean match = baseConstraints.stream().anyMatch(baseConstraint -> baseConstraint instanceof NotNullConstraint);
        if (match) {
            return true;
        }
        return null;
    }

    private Boolean isPrimary(List<BaseConstraint> baseConstraints) {
        return baseConstraints.stream().anyMatch(baseConstraint -> baseConstraint instanceof PrimaryConstraint);
    }

    @Override
    public Node visitColumn_constraint(Column_constraintContext ctx) {
        Optional<Identifier> optionalIdentifier = ParserHelper.visitIfPresent(this, ctx.name(), Identifier.class);
        Identifier identifier = null;
        identifier = optionalIdentifier.orElseGet(IdentifierUtil::sysIdentifier);

        if (ctx.PRIMARY_() != null) {
            return new PrimaryConstraint(identifier, Lists.newArrayList());
        }

        if (ctx.NOT_() != null && ctx.NULL_() != null) {
            return new NotNullConstraint(identifier);
        }
        return null;
    }

    @Override
    public Node visitTable_constraint(Table_constraintContext ctx) {
        NameContext name = ctx.name();
        Identifier constraintName = null;
        if (name != null) {
            constraintName = (Identifier)visit(name);
        } else {
            constraintName = IdentifierUtil.sysIdentifier();
        }
        if (ctx.PRIMARY_() != null) {
            List<Identifier> list = ParserHelper.visit(this, ctx.indexed_column(), Identifier.class);
            return new PrimaryConstraint(constraintName, list);
        }
        if (ctx.FOREIGN_() != null) {
            List<Identifier> list = ParserHelper.visit(this, ctx.column_name(), Identifier.class);
            Foreign_key_clauseContext foreignKeyClauseContext = ctx.foreign_key_clause();
            QualifiedName qualifiedName = (QualifiedName)visit(foreignKeyClauseContext.foreign_table());
            List<Identifier> referenceColumns = ParserHelper.visit(this, foreignKeyClauseContext.column_name(), Identifier.class);
            //如果为空，那么用左表的列输出
            if (referenceColumns.isEmpty()) {
                referenceColumns = new CopyOnWriteArrayList<>(list);
            }
            return new DimConstraint(
                constraintName, list,
                qualifiedName,
                referenceColumns
            );
        }
        return super.visitTable_constraint(ctx);
    }

    @Override
    public Node visitForeign_table(Foreign_tableContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.any_name());
        return QualifiedName.of(Lists.newArrayList(identifier));
    }

    @Override
    public Node visitAny_name(Any_nameContext ctx) {
        if (ctx.IDENTIFIER() != null) {
            String text = ctx.IDENTIFIER().getText();
            Identifier identifier = new Identifier(text);
            if (identifier.isDelimited()) {
                return new Identifier(StripUtils.strip(text), true);
            }
            return identifier;
        }
        if (ctx.keyword() != null) {
            return new Identifier(ctx.keyword().getText());
        }
        if (ctx.STRING_LITERAL() != null) {
            StringLiteral stringLiteral = new StringLiteral(StripUtils.strip(ctx.STRING_LITERAL().getText()));
            return new Identifier(stringLiteral.getValue());
        }
        return new Identifier(ctx.getText());
    }

    @Override
    public Node visitType_name(Type_nameContext ctx) {
        List<Identifier> list = ParserHelper.visit(this, ctx.name(), Identifier.class);
        List<DataTypeParameter> dataTypeParameter = null;
        if (ctx.signed_number() != null) {
            dataTypeParameter = ParserHelper.visit(this, ctx.signed_number(), DataTypeParameter.class);
        }
        Identifier first = list.get(0);
        return new GenericDataType(first.getValue(), dataTypeParameter);
    }
}
