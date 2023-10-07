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

package com.aliyun.fastmodel.transform.mysql.parser;

import java.util.List;
import java.util.Objects;
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
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.script.RefDirection;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.CloneTable;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DefaultValueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.SortType;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.exception.ReverseUnsupportedOperationException;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.AlterByAddForeignKeyContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.CheckTableConstraintContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.CollectionDataTypeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.CollectionOptionsContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.ColumnConstraintContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.ColumnCreateTableContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.ColumnDeclarationContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.ColumnDefinitionContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.CommentColumnConstraintContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.CopyCreateTableContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.CreateDefinitionsContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.DecimalLiteralContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.DefaultColumnConstraintContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.DimensionDataTypeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.ForeignKeyTableConstraintContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.IndexColumnNameContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.IndexColumnNamesContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.IndexOptionContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.LengthOneDimensionContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.LongVarbinaryDataTypeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.LongVarcharDataTypeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.NationalStringDataTypeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.NationalVaryingStringDataTypeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.NullColumnConstraintContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.PrimaryKeyColumnConstraintContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.PrimaryKeyTableConstraintContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.QueryCreateTableContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.ReferenceDefinitionContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.RootContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.SimpleDataTypeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.SimpleIndexDeclarationContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.SpatialDataTypeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.SpecialIndexDeclarationContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.SqlStatementsContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.StringDataTypeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.StringLiteralContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableNameContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionAutoIncrementContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionAverageContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionCharsetContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionChecksumContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionCollateContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionCommentContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionCompressionContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionConnectionContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionDataDirectoryContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionDelayContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionEncryptionContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionEngineContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionIndexDirectoryContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionInsertMethodContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionKeyBlockSizeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionMaxRowsContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionMinRowsContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionPackKeysContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionPasswordContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionPersistentContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionRecalculationContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionRowFormatContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionSamplePageContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionTableTypeContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionTablespaceContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.TableOptionUnionContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.UidContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.UniqueKeyColumnConstraintContext;
import com.aliyun.fastmodel.transform.mysql.parser.MySqlParser.UniqueKeyTableConstraintContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getIdentifier;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;
import static com.aliyun.fastmodel.common.parser.ParserHelper.visitIfPresent;

/**
 * MysqlAstBuilder
 *
 * @author panguanjing
 * @date 2021/8/28
 */
public class MysqlAstBuilder extends MySqlParserBaseVisitor<Node> {
    public static final String COMMENT = "comment";
    private final ReverseContext reverseContext;

    public MysqlAstBuilder(ReverseContext reverseContext) {
        this.reverseContext = reverseContext;
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
    public Node visitCopyCreateTable(CopyCreateTableContext ctx) {
        return new CloneTable(
            CreateElement.builder()
                .qualifiedName(getQualifiedName(ctx.tableName(0)))
                .notExists(ctx.ifNotExists() != null)
                .build(),
            reverseContext.getReverseTableType(),
            getQualifiedName(ctx.tableName(1))
        );
    }

    @Override
    public Node visitQueryCreateTable(QueryCreateTableContext ctx) {
        return super.visitQueryCreateTable(ctx);
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
            columnDefinitions = nodes.stream().filter(x -> x instanceof ColumnDefinition).map(x -> {
                return (ColumnDefinition)x;
            }).collect(Collectors.toList());
            constraints = nodes.stream().filter(x -> x instanceof BaseConstraint).map(x -> {
                return (BaseConstraint)x;
            }).collect(Collectors.toList());

            tableIndexList = nodes.stream().filter(x -> x instanceof TableIndex).map(x -> {
                return (TableIndex)x;
            }).collect(Collectors.toList());
        }

        PartitionedBy partitionedBy = ParserHelper.visitIfPresent(this, ctx.partitionDefinitions(),
            PartitionedBy.class).orElse(null);

        Property commentProperties = getCommentProperty(properties);

        List<Property> otherProperty = properties.stream().filter(p -> {
            return !p.getName().equalsIgnoreCase(COMMENT);
        }).collect(Collectors.toList());

        Comment comment = commentProperties != null ? new Comment(commentProperties.getValue())
            : null;
        CreateTable createTable = CreateTable.builder()
            .tableName(getQualifiedName(ctx.tableName()))
            .detailType(reverseContext.getReverseTableType())
            .columns(columnDefinitions)
            .constraints(constraints)
            .properties(otherProperty)
            .partition(partitionedBy)
            .comment(
                comment)
            .tableIndex(tableIndexList)
            .ifNotExist(ctx.ifNotExists() != null)
            .build();
        return createTable;
    }

    private Property getCommentProperty(List<Property> properties) {
        if (CollectionUtils.isEmpty(properties)) {
            return null;
        }
        return properties.stream()
            .filter(property -> {
                return StringUtils.equalsIgnoreCase(property.getName(), COMMENT);
            })
            .findFirst()
            .orElse(null);
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
            c = list.stream().filter(x -> x instanceof Comment).map(x -> {
                return (Comment)x;
            }).findFirst().orElse(null);
            PrimaryConstraint primaryConstraint = list.stream().filter(x -> x instanceof PrimaryConstraint).map(x -> {
                return (PrimaryConstraint)x;
            }).findFirst().orElse(null);
            if (primaryConstraint != null) {
                primary = primaryConstraint.getEnable();
            }
            NotNullConstraint notNullConstraint = list.stream().filter(x -> x instanceof NotNullConstraint).map(x -> {
                return (NotNullConstraint)x;
            }).findFirst().orElse(null);
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
    public Node visitStringDataType(StringDataTypeContext ctx) {
        List<DataTypeParameter> arguments = ImmutableList.of();
        if (ctx.lengthOneDimension() != null) {
            DataTypeParameter dataTypeParameter = (DataTypeParameter)visit(ctx.lengthOneDimension());
            arguments = ImmutableList.of(dataTypeParameter);
        }
        return new GenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            ctx.typeName.getText(),
            arguments
        );
    }

    @Override
    public Node visitLengthOneDimension(LengthOneDimensionContext ctx) {
        return new NumericParameter(ctx.decimalLiteral().getText());
    }

    @Override
    public Node visitNationalStringDataType(NationalStringDataTypeContext ctx) {
        return super.visitNationalStringDataType(ctx);
    }

    @Override
    public Node visitNationalVaryingStringDataType(NationalVaryingStringDataTypeContext ctx) {
        return super.visitNationalVaryingStringDataType(ctx);
    }

    @Override
    public Node visitDimensionDataType(DimensionDataTypeContext ctx) {
        DataTypeParameter dataTypeParameter =
            visitIfPresent(this, ctx.lengthOneDimension(), DataTypeParameter.class)
                .orElse(null);
        List<DataTypeParameter> list = ImmutableList.of();
        if (dataTypeParameter != null) {
            list = ImmutableList.of(dataTypeParameter);
        }
        return new GenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            ctx.typeName.getText(),
            list
        );
    }

    @Override
    public Node visitSimpleDataType(SimpleDataTypeContext ctx) {
        return new GenericDataType(new Identifier(ctx.typeName.getText()), ImmutableList.of());
    }

    @Override
    public Node visitCollectionDataType(CollectionDataTypeContext ctx) {
        List<DataTypeParameter> list = ImmutableList.of();
        CollectionOptionsContext collectionOptionsContext = ctx.collectionOptions();
        if (collectionOptionsContext != null) {
            list = collectionOptionsContext.STRING_LITERAL().stream().map(x -> {
                DataTypeParameter dataTypeParameter = new NumericParameter(StripUtils.strip(x.getText()));
                return dataTypeParameter;
            }).collect(Collectors.toList());
        }
        return new GenericDataType(
            new Identifier(ctx.typeName.getText()),
            list
        );
    }

    @Override
    public Node visitSpatialDataType(SpatialDataTypeContext ctx) {
        return new GenericDataType(new Identifier(ctx.typeName.getText()));
    }

    @Override
    public Node visitLongVarcharDataType(LongVarcharDataTypeContext ctx) {
        TerminalNode aLong = ctx.LONG();
        String type = aLong.getText();
        if (ctx.VARCHAR() != null) {
            type = type + " " + ctx.VARCHAR().getText();
        }
        return new GenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            type,
            ImmutableList.of()
        );
    }

    @Override
    public Node visitLongVarbinaryDataType(LongVarbinaryDataTypeContext ctx) {
        return new GenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            "LONG VARBINARY",
            ImmutableList.of()
        );
    }

    @Override
    public Node visitNullColumnConstraint(NullColumnConstraintContext ctx) {
        TerminalNode not = ctx.nullNotnull().NOT();
        return not != null ? new NotNullConstraint(IdentifierUtil.sysIdentifier()) :
            new NotNullConstraint(IdentifierUtil.sysIdentifier(), false);
    }

    @Override
    public Node visitDefaultColumnConstraint(DefaultColumnConstraintContext ctx) {
        BaseLiteral baseLiteral = (BaseLiteral)visit(ctx.defaultValue());
        return new DefaultValueConstraint(
            IdentifierUtil.sysIdentifier(),
            baseLiteral
        );
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

    @Override
    public Node visitPrimaryKeyTableConstraint(PrimaryKeyTableConstraintContext ctx) {
        //primary key table constraint
        UidContext name = ctx.name;
        Identifier constraintName = null;
        if (name != null) {
            constraintName = getIdentifier(name);
        } else {
            constraintName = IdentifierUtil.sysIdentifier();
        }
        List<Identifier> columnName = ParserHelper.visit(this, ctx.indexColumnNames().indexColumnName(),
                IndexColumnName.class)
            .stream()
            .map(IndexColumnName::getColumnName)
            .collect(Collectors.toList());
        return new PrimaryConstraint(constraintName, columnName);
    }

    @Override
    public Node visitIndexColumnName(IndexColumnNameContext ctx) {
        Identifier col = null;
        if (ctx.uid() != null) {
            col = (Identifier)visit(ctx.uid());
        } else if (ctx.STRING_LITERAL() != null) {
            col = new Identifier(StripUtils.strip(ctx.STRING_LITERAL().getText()));
        }
        DecimalLiteralContext decimalLiteralContext = ctx.decimalLiteral();
        LongLiteral colLength = null;
        if (decimalLiteralContext != null) {
            colLength = new LongLiteral(decimalLiteralContext.getText());

        }
        SortType sortType = null;
        if (ctx.sortType != null) {
            sortType = SortType.valueOf(ctx.sortType.getText().toUpperCase());
        }
        return new IndexColumnName(col, colLength, sortType);
    }

    @Override
    public Node visitUniqueKeyTableConstraint(UniqueKeyTableConstraintContext ctx) {
        //unique key table constraint
        UidContext name = ctx.name;
        Identifier constraintName = null;
        if (name != null) {
            constraintName = getIdentifier(name);
        } else {
            constraintName = IdentifierUtil.sysIdentifier();
        }
        IndexColumnNamesContext indexColumnNamesContext = ctx.indexColumnNames();
        List<Identifier> columnName =
            ParserHelper.visit(this, ctx.indexColumnNames().indexColumnName(), IndexColumnName.class)
                .stream()
                .map(IndexColumnName::getColumnName)
                .collect(Collectors.toList());
        return new UniqueConstraint(constraintName, columnName, true);
    }

    @Override
    public Node visitForeignKeyTableConstraint(ForeignKeyTableConstraintContext ctx) {
        //foreign key constraint
        UidContext name = ctx.name;
        Identifier constraintName = null;
        if (name != null) {
            constraintName = getIdentifier(name);
        } else {
            constraintName = IdentifierUtil.sysIdentifier();
        }
        List<Identifier> columnName = ParserHelper.visit(this, ctx.indexColumnNames().indexColumnName(),
            IndexColumnName.class).stream().map(IndexColumnName::getColumnName).collect(Collectors.toList());

        ReferenceDefinitionContext referenceDefinitionContext = ctx.referenceDefinition();
        QualifiedName refereTable = null;
        List<Identifier> refereColumns = null;
        if (referenceDefinitionContext != null) {
            refereTable = getQualifiedName(referenceDefinitionContext.tableName());
            refereColumns = ParserHelper.visit(this, referenceDefinitionContext.indexColumnNames().indexColumnName(),
                IndexColumnName.class).stream().map(IndexColumnName::getColumnName).collect(Collectors.toList());
        }
        DimConstraint dimConstraint = new DimConstraint(
            constraintName,
            columnName,
            refereTable,
            refereColumns
        );
        return dimConstraint;
    }

    @Override
    public Node visitCheckTableConstraint(CheckTableConstraintContext ctx) {
        //check table constraint
        return super.visitCheckTableConstraint(ctx);
    }

    @Override
    public Node visitSimpleIndexDeclaration(SimpleIndexDeclarationContext ctx) {
        List<IndexColumnName> visit = ParserHelper.visit(this, ctx.indexColumnNames().indexColumnName(),
            IndexColumnName.class);
        List<Property> list = ImmutableList.of();
        List<IndexOptionContext> indexOptionContexts = ctx.indexOption();
        if (indexOptionContexts != null) {
            list = ParserHelper.visit(this, indexOptionContexts, Property.class);
        }
        TableIndex tableIndex = new TableIndex(
            (Identifier)visit(ctx.uid()),
            visit,
            list.stream().filter(Objects::nonNull).collect(Collectors.toList())
        );
        return tableIndex;
    }

    @Override
    public Node visitIndexOption(IndexOptionContext ctx) {
        TerminalNode comment = ctx.COMMENT();
        if (comment != null) {
            return new Property(comment.getText(), StripUtils.strip(ctx.STRING_LITERAL().getText()));
        }
        if (ctx.KEY_BLOCK_SIZE() != null) {
            return new Property(ctx.KEY_BLOCK_SIZE().getText(), ctx.fileSizeLiteral().getText());
        }
        if (ctx.WITH() != null && ctx.PARSER() != null) {
            return new Property("WITH PARSER", ctx.uid().getText());
        }
        if (ctx.INVISIBLE() != null) {
            return new Property(ctx.INVISIBLE().getText(), "1");
        }
        if (ctx.VISIBLE() != null) {
            return new Property(ctx.INVISIBLE().getText(), "1");
        }
        if (ctx.indexType() != null) {
            return new Property("indexType", ctx.indexType().getText());
        }
        return null;
    }

    @Override
    public Node visitSpecialIndexDeclaration(SpecialIndexDeclarationContext ctx) {
        return super.visitSpecialIndexDeclaration(ctx);
    }

    @Override
    public Node visitTableOptionEngine(TableOptionEngineContext ctx) {
        Property property = new Property(ctx.ENGINE().getText(), ctx.engineName().getText());
        return property;
    }

    @Override
    public Node visitTableOptionAutoIncrement(TableOptionAutoIncrementContext ctx) {
        Property property = new Property(ctx.AUTO_INCREMENT().getText(), ctx.decimalLiteral().getText());
        return property;
    }

    @Override
    public Node visitTableOptionAverage(TableOptionAverageContext ctx) {
        return new Property(ctx.AVG_ROW_LENGTH().getText(), ctx.decimalLiteral().getText());
    }

    @Override
    public Node visitTableOptionCharset(TableOptionCharsetContext ctx) {
        Property property = new Property(ctx.charSet().getText(), ctx.charsetName().getText());
        return property;
    }

    @Override
    public Node visitTableOptionChecksum(TableOptionChecksumContext ctx) {
        if (ctx.CHECKSUM() != null) {
            return new Property(ctx.CHECKSUM().getText(), ctx.boolValue.getText());
        } else if (ctx.PAGE_CHECKSUM() != null) {
            return new Property(ctx.PAGE_CHECKSUM().getText(), ctx.boolValue.getText());
        }
        return null;
    }

    @Override
    public Node visitTableOptionCollate(TableOptionCollateContext ctx) {
        Property property = new Property(ctx.COLLATE().getText(), ctx.collationName().getText());
        return property;
    }

    @Override
    public Node visitTableOptionComment(TableOptionCommentContext ctx) {
        String text = ctx.STRING_LITERAL().getText();
        Property property = new Property(ctx.COMMENT().getText(), StripUtils.strip(text));
        return property;
    }

    @Override
    public Node visitStringLiteral(StringLiteralContext ctx) {
        return new StringLiteral(getLocation(ctx), getOrigin(ctx), StripUtils.strip(ctx.getText()));
    }

    @Override
    public Node visitTableOptionCompression(TableOptionCompressionContext ctx) {
        return new Property(ctx.COMPRESSION().getText(), StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public Node visitTableOptionConnection(TableOptionConnectionContext ctx) {
        return new Property(ctx.CONNECTION().getText(), StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public Node visitTableOptionDataDirectory(TableOptionDataDirectoryContext ctx) {
        return new Property("DATA DICTIONRY", StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public Node visitTableOptionDelay(TableOptionDelayContext ctx) {
        return new Property("DELAY_KEY_WRITE", StripUtils.strip(ctx.boolValue.getText()));
    }

    @Override
    public Node visitTableOptionEncryption(TableOptionEncryptionContext ctx) {
        return new Property("ENCRYPTION", StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public Node visitTableOptionIndexDirectory(TableOptionIndexDirectoryContext ctx) {
        return new Property("INDEX DIRECTORY", StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public Node visitTableOptionInsertMethod(TableOptionInsertMethodContext ctx) {
        return new Property("INSERT_METHOD", ctx.insertMethod.getText());
    }

    @Override
    public Node visitTableOptionKeyBlockSize(TableOptionKeyBlockSizeContext ctx) {
        return new Property(ctx.KEY_BLOCK_SIZE().getText(), ctx.fileSizeLiteral().getText());
    }

    @Override
    public Node visitTableOptionMaxRows(TableOptionMaxRowsContext ctx) {
        return new Property(ctx.MAX_ROWS().getText(), ctx.decimalLiteral().getText());
    }

    @Override
    public Node visitTableOptionMinRows(TableOptionMinRowsContext ctx) {
        return new Property(ctx.MIN_ROWS().getText(), ctx.decimalLiteral().getText());
    }

    @Override
    public Node visitTableOptionPackKeys(TableOptionPackKeysContext ctx) {
        return new Property(ctx.PACK_KEYS().getText(), StripUtils.strip(ctx.extBoolValue.getText()));
    }

    @Override
    public Node visitTableOptionPassword(TableOptionPasswordContext ctx) {
        return new Property(ctx.PASSWORD().getText(), StripUtils.strip(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public Node visitTableOptionRowFormat(TableOptionRowFormatContext ctx) {
        return new Property(ctx.ROW_FORMAT().getText(), ctx.rowFormat.getText());
    }

    @Override
    public Node visitTableOptionRecalculation(TableOptionRecalculationContext ctx) {
        return new Property(ctx.STATS_AUTO_RECALC().getText(), StripUtils.strip(ctx.extBoolValue.getText()));
    }

    @Override
    public Node visitTableOptionPersistent(TableOptionPersistentContext ctx) {
        return new Property(ctx.STATS_PERSISTENT().getText(), StripUtils.strip(ctx.extBoolValue.getText()));
    }

    @Override
    public Node visitTableOptionSamplePage(TableOptionSamplePageContext ctx) {
        return new Property(ctx.STATS_SAMPLE_PAGES().getText(), StripUtils.strip(ctx.decimalLiteral().getText()));
    }

    @Override
    public Node visitTableOptionTablespace(TableOptionTablespaceContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.uid());
        return new Property(ctx.TABLESPACE().getText(), identifier.getValue());
    }

    @Override
    public Node visitTableOptionTableType(TableOptionTableTypeContext ctx) {
        return new Property(ctx.TABLE_TYPE().getText(), ctx.tableType().getText());
    }

    @Override
    public Node visitAlterByAddForeignKey(AlterByAddForeignKeyContext ctx) {
        QualifiedName tableName = getQualifiedName((TableNameContext)ctx.value);
        Identifier constraint = null;
        List<Identifier> columns = ImmutableList.of();
        if (ctx.name != null) {
            constraint = (Identifier)visit(ctx.name);
        } else if (ctx.indexName != null) {
            constraint = (Identifier)visit(ctx.indexName);
        } else {
            constraint = IdentifierUtil.sysIdentifier();
        }
        //columns parse
        IndexColumnNamesContext indexColumnNamesContext = ctx.indexColumnNames();
        if (indexColumnNamesContext != null) {
            List<IndexColumnName> indexColumnNames =
                ParserHelper.visit(this, indexColumnNamesContext.indexColumnName(), IndexColumnName.class);
            columns = indexColumnNames
                .stream()
                .map(IndexColumnName::getColumnName)
                .collect(Collectors.toList());
        }

        QualifiedName reference = getQualifiedName(ctx.referenceDefinition().tableName());
        List<Identifier> referenceColumns = ImmutableList.of();
        if (ctx.referenceDefinition().indexColumnNames() != null) {
            List<IndexColumnName> indexColumnNames = ParserHelper.visit(this,
                ctx.referenceDefinition().indexColumnNames().indexColumnName(),
                IndexColumnName.class);
            referenceColumns = indexColumnNames
                .stream()
                .map(IndexColumnName::getColumnName)
                .collect(Collectors.toList());
        }
        DimConstraint dimConstraint = new DimConstraint(
            constraint,
            columns,
            reference,
            referenceColumns
        );
        switch (reverseContext.getReverseRelationStrategy()) {
            case DDL:
                AddConstraint addConstraint = new AddConstraint(
                    tableName,
                    dimConstraint
                );
                return addConstraint;
            case SCRIPT:
                return new RefRelation(
                    QualifiedName.of(
                        Lists.newArrayList(constraint)),
                    new RefObject(tableName, columns, null),
                    new RefObject(reference, referenceColumns, null),
                    RefDirection.LEFT_DIRECTION_RIGHT
                );
            default:
                throw new ReverseUnsupportedOperationException(ParserHelper.getOrigin(ctx),
                    "unsupported the strategy:" + reverseContext.getReverseRelationStrategy());
        }
    }

    @Override
    public Node visitTableOptionUnion(TableOptionUnionContext ctx) {
        List<QualifiedName> list = ParserHelper.visit(this, ctx.tables().tableName(), QualifiedName.class);
        return new Property(ctx.UNION().getText(),
            list.stream().map(QualifiedName::toString).collect(Collectors.joining(",")));
    }

    private QualifiedName getQualifiedName(TableNameContext tableName) {
        List<Identifier> list = ParserHelper.visit(this, tableName.fullId().uid(), Identifier.class);
        return QualifiedName.of(list);
    }

    @Override
    public Node visitUid(MySqlParser.UidContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

}
