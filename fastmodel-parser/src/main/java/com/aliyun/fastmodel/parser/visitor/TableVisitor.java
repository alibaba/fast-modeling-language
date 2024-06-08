/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.parser.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.CloneTable;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateCodeTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateIndex;
import com.aliyun.fastmodel.core.tree.statement.table.CreateOdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.DropIndex;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.ColumnGroupConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelDefine;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.RedundantConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.TimePeriodConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.SortType;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AddConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColGroupConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnDefinitionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnNameTypeOrConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateDimKeyContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateIndexContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateLevelConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateTableStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DimDetailTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropIndexContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropTableStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DwDetailTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.FactDetailTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IndexColumnNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.LevelColPathContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.LikeTablePredicateContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.PrimaryConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RedundantConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameTableContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetTableAliasedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetTableCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetTablePropertiesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TableConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TableIndexContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TableTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TimePeriodConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.UnSetTablePropertiesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.UniqueConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelLexer;
import com.aliyun.fastmodel.parser.visitor.param.TableParamObject;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.Token;

import static java.util.stream.Collectors.toList;

/**
 * 表的visitor
 *
 * @author panguanjing
 * @date 2021/4/9
 */
@SubVisitor
public class TableVisitor extends AstBuilder {
    @Override
    public Node visitCreateTableStatement(CreateTableStatementContext ctx) {
        List<ColumnDefinition> columnDefines = new ArrayList<>();
        PartitionedBy partitionedBy = null;
        if (ctx.tablePartition() != null) {
            List<ColumnDefinition> list = visit(
                ctx.tablePartition().columnNameTypeOrConstraintList().columnNameTypeOrConstraint(),
                ColumnDefinition.class);
            partitionedBy = new PartitionedBy(list);
        }
        List<BaseConstraint> constraints = new ArrayList<>();
        List<TableIndex> tableIndexList = new ArrayList<>();
        TableParamObject tableParamObject = new TableParamObject(
            partitionedBy != null ? partitionedBy.getColumnDefinitions() : ImmutableList.of(),
            columnDefines, constraints, tableIndexList);
        tableParamObject.builderColumns(ctx, this);
        QualifiedName tableName = getQualifiedName(ctx.tableName());
        List<Property> properties = getProperties(ctx.setProperties());
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);

        boolean notExists = ctx.ifNotExists() != null;
        TableDetailType tableDetailType = getDetailType(ctx.tableType());
        TableType parent = tableDetailType != null ? tableDetailType.getParent() : null;
        boolean replace = ctx.replace() != null;

        //if like define use
        LikeTablePredicateContext likeTablePredicateContext = ctx.likeTablePredicate();
        if (likeTablePredicateContext != null) {
            QualifiedName sourceTableName = getQualifiedName(likeTablePredicateContext.tableName());
            return new CloneTable(
                CreateElement.builder()
                    .qualifiedName(tableName)
                    .notExists(notExists)
                    .aliasedName(aliasedName)
                    .properties(properties)
                    .comment(comment)
                    .createOrReplace(replace)
                    .build(),
                tableDetailType,
                sourceTableName
            );
        }
        CreateTable.TableBuilder<? extends TableBuilder> builder = getTableBuilder(parent);
        return builder.tableName(tableName)
            .detailType(tableDetailType)
            .ifNotExist(notExists)
            .columns(columnDefines)
            .constraints(constraints)
            .properties(properties)
            .comment(comment)
            .partition(partitionedBy)
            .createOrReplace(replace)
            .aliasedName(aliasedName)
            .tableIndex(tableIndexList)
            .build();
    }

    private TableBuilder<? extends TableBuilder> getTableBuilder(TableType parent) {
        if (parent == null) {
            return CreateTable.builder();
        }
        if (parent == TableType.DIM) {
            return CreateDimTable.builder();
        } else if (parent == TableType.ADS) {
            return CreateAdsTable.builder();
        } else if (parent == TableType.FACT) {
            return CreateFactTable.builder();
        } else if (parent == TableType.DWS) {
            return CreateDwsTable.builder();
        } else if (parent == TableType.CODE) {
            return CreateCodeTable.builder();
        } else if (parent == TableType.ODS) {
            return CreateOdsTable.builder();
        }
        throw new ParseException("unsupported table type with:" + parent);
    }

    @Override
    public Node visitRenameTable(RenameTableContext ctx) {
        return new RenameTable(getQualifiedName(ctx.tableName()),
            getQualifiedName(ctx.alterStatementSuffixRename().qualifiedName()));
    }

    @Override
    public Node visitSetTableProperties(SetTablePropertiesContext ctx) {
        List<Property> visit = getProperties(ctx.setProperties());
        return new SetTableProperties(getQualifiedName(ctx.tableName()), visit);
    }

    @Override
    public Node visitUnSetTableProperties(UnSetTablePropertiesContext ctx) {
        List<Identifier> list = getUnSetProperties(ctx.unSetProperties());
        return new UnSetTableProperties(getQualifiedName(ctx.tableName()),
            list.stream().map(Identifier::getValue).collect(toList())
        );
    }

    @Override
    public Node visitDropTableStatement(DropTableStatementContext ctx) {
        return new DropTable(getQualifiedName(ctx.tableName()), ctx.ifExists() != null);
    }

    @Override
    public Node visitSetTableComment(SetTableCommentContext ctx) {
        return new SetTableComment(getQualifiedName(ctx.tableName()),
            (Comment)visit(ctx.alterStatementSuffixSetComment()));
    }

    @Override
    public Node visitAddConstraint(AddConstraintContext ctx) {
        BaseConstraint baseConstraint = (BaseConstraint)visit(ctx.alterStatementSuffixAddConstraint());
        return new AddConstraint(getQualifiedName(ctx.tableName()), baseConstraint);
    }

    @Override
    public Node visitDropConstraint(DropConstraintContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.alterStatementSuffixDropConstraint());
        return new DropConstraint(getQualifiedName(ctx.tableName()), identifier);
    }

    @Override
    public Node visitTableIndex(TableIndexContext ctx) {
        List<IndexColumnName> list = visit(ctx.indexColumnNames().indexColumnName(), IndexColumnName.class);
        List<Property> indexProperties = ImmutableList.of();
        if (ctx.indexOption() != null) {
            indexProperties = getProperties(ctx.indexOption().setProperties());
        }
        return new TableIndex(
            (Identifier)visit(ctx.identifier()),
            list,
            indexProperties
        );
    }

    @Override
    public Node visitIndexColumnName(IndexColumnNameContext ctx) {
        LongLiteral literal = null;
        if (ctx.numberLiteral() != null) {
            literal = (LongLiteral)visit(ctx.numberLiteral());
        }
        SortType sortType = null;
        if (ctx.sortType != null) {
            sortType = SortType.valueOf(ctx.sortType.getText().toUpperCase());
        }
        return new IndexColumnName(
            (Identifier)visit(ctx.columnName()),
            literal,
            sortType
        );
    }

    @Override
    public Node visitUniqueConstraint(UniqueConstraintContext ctx) {
        Identifier constraintName;
        if (ctx.identifier() != null) {
            constraintName = (Identifier)visit(ctx.identifier());
        } else {
            constraintName = IdentifierUtil.sysIdentifier();
        }
        List<Identifier> columns = visit(ctx.columnParenthesesList().columnNameList().columnName(),
            Identifier.class);
        return new UniqueConstraint(constraintName, columns, true);
    }

    @Override
    public Node visitColumnNameTypeOrConstraint(ColumnNameTypeOrConstraintContext ctx) {
        TableConstraintContext tableConstraintContext = ctx.tableConstraint();
        if (tableConstraintContext != null) {
            return visit(tableConstraintContext);
        }
        ColumnDefinitionContext columnDefinitionContext = ctx.columnDefinition();
        if (columnDefinitionContext != null) {
            return visit(columnDefinitionContext);
        }
        return super.visitColumnNameTypeOrConstraint(ctx);
    }

    @Override
    public Node visitCreateDimKey(CreateDimKeyContext ctx) {
        Identifier identifier = visitIfPresent(ctx.identifier(), Identifier.class).orElse(
            IdentifierUtil.sysIdentifier());

        List<Identifier> columns = ImmutableList.of();
        if (ctx.columnParenthesesList() != null) {
            columns = visit(ctx.columnParenthesesList().columnNameList().columnName(), Identifier.class);
        }
        List<Identifier> referenceList = ImmutableList.of();
        if (ctx.referenceColumnList() != null) {
            referenceList = visit(ctx.referenceColumnList().columnParenthesesList().columnNameList().columnName(),
                Identifier.class);
        }
        return new DimConstraint(
            identifier,
            columns,
            getQualifiedName(ctx.tableName()),
            referenceList
        );
    }

    @Override
    public Node visitColGroupConstraint(ColGroupConstraintContext ctx) {
        Identifier identifier = visitIfPresent(ctx.identifier(), Identifier.class).orElse(
            IdentifierUtil.sysIdentifier());
        List<Identifier> columns = ImmutableList.of();
        if (ctx.columnParenthesesList() != null) {
            columns = visit(ctx.columnParenthesesList().columnNameList().columnName(), Identifier.class);
        }
        return new ColumnGroupConstraint(
            identifier,
            columns
        );
    }

    @Override
    public Node visitCreateLevelConstraint(CreateLevelConstraintContext ctx) {
        Identifier identifier = visitIfPresent(ctx.identifier(), Identifier.class).orElse(
            IdentifierUtil.sysIdentifier());
        List<LevelDefine> levelDefines = ImmutableList.of();
        if (ctx.levelColParenthesesList() != null) {
            levelDefines = visit(ctx.levelColParenthesesList().levelCol().levelColPath(), LevelDefine.class);
        }
        Optional<Comment> comment = visitIfPresent(ctx.comment(), Comment.class);
        return new LevelConstraint(identifier, levelDefines, comment.orElse(null));
    }

    @Override
    public Node visitTimePeriodConstraint(TimePeriodConstraintContext ctx) {
        List<Identifier> list = visit(ctx.columnParenthesesList().columnNameList().columnName(), Identifier.class);
        Identifier identifier = visitIfPresent(ctx.identifier(), Identifier.class).orElse(
            IdentifierUtil.sysIdentifier());
        return new TimePeriodConstraint(identifier, list);
    }

    @Override
    public Node visitRedundantConstraint(RedundantConstraintContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        Identifier column = (Identifier)visit(ctx.columnName());
        QualifiedName qualifiedName = getQualifiedName(ctx.joinColumn().qualifiedName());
        List<Identifier> referenceColumns = ImmutableList.of();
        if (ctx.referenceColumnList() != null) {
            referenceColumns = visit(ctx.referenceColumnList()
                    .columnParenthesesList()
                    .columnNameList()
                    .columnName(),
                Identifier.class);
        }
        return new RedundantConstraint(identifier, column, qualifiedName, referenceColumns);
    }

    @Override
    public Node visitLevelColPath(LevelColPathContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.columnName());
        List<Identifier> identifiers = ImmutableList.of();
        if (ctx.columnParenthesesList() != null) {
            identifiers = visit(ctx.columnParenthesesList().columnNameList().columnName(), Identifier.class);
        }
        return new LevelDefine(identifier, identifiers);
    }

    @Override
    public Node visitSetTableAliasedName(SetTableAliasedNameContext ctx) {
        return new SetTableAliasedName(
            getQualifiedName(ctx.tableName()),
            (AliasedName)visit(ctx.setAliasedName())
        );
    }

    @Override
    public Node visitPrimaryConstraint(PrimaryConstraintContext ctx) {
        Optional<Identifier> identifier = visitIfPresent(ctx.identifier(), Identifier.class);
        List<Identifier> visit = visit(ctx.columnParenthesesList().columnNameList().columnName(), Identifier.class);
        return new PrimaryConstraint(
            identifier.orElse(IdentifierUtil.sysIdentifier()),
            visit, true);
    }

    @Override
    public Node visitCreateIndex(CreateIndexContext ctx) {
        List<Property> list = ImmutableList.of();
        if (ctx.indexOption() != null) {
            list = visit(ctx.indexOption().setProperties().tableProperties().keyValueProperty(), Property.class);
        }
        return new CreateIndex(
            getQualifiedName(ctx.qualifiedName()),
            getQualifiedName(ctx.tableName()),
            visit(ctx.indexColumnNames().indexColumnName(), IndexColumnName.class),
            list
        );
    }

    @Override
    public Node visitDropIndex(DropIndexContext ctx) {
        return new DropIndex(
            getQualifiedName(ctx.qualifiedName()),
            getQualifiedName(ctx.tableName())
        );
    }

    private TableDetailType getDetailType(TableTypeContext typeContext) {
        if (typeContext == null) {
            return null;
        }
        Token type = typeContext.type;
        switch (type.getType()) {
            case FastModelLexer.KW_FACT:
                FactDetailTypeContext factDetailTypeContext = typeContext.factDetailType();
                if (factDetailTypeContext == null) {
                    return TableDetailType.TRANSACTION_FACT;
                } else {
                    return TableDetailType.getByCode(factDetailTypeContext.getText(), TableType.FACT);
                }
            case FastModelLexer.KW_DIM:
                DimDetailTypeContext dimDetailTypeContext = typeContext.dimDetailType();
                if (dimDetailTypeContext == null) {
                    return TableDetailType.NORMAL_DIM;
                } else {
                    return TableDetailType.getByCode(dimDetailTypeContext.getText(), TableType.DIM);
                }
            case FastModelLexer.KW_CODE:
                return TableDetailType.CODE;
            case FastModelLexer.KW_DWS:
                DwDetailTypeContext dwDetailTypeContext = typeContext.dwDetailType();
                if (dwDetailTypeContext == null) {
                    return TableDetailType.DWS;
                }
                return TableDetailType.ADVANCED_DWS;
            case FastModelLexer.KW_ADS:
                return TableDetailType.ADS;
            case FastModelLexer.KW_ODS:
                return TableDetailType.ODS;
            default:
                throw new IllegalArgumentException("can't find the DetailType:" + type.getType());
        }
    }

}
