package com.aliyun.fastmodel.transform.oceanbase.parser;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.ListNode;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.BitOperationExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.IsConditionExpression;
import com.aliyun.fastmodel.core.tree.expr.LogicalBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.Cast;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.InPredicate;
import com.aliyun.fastmodel.core.tree.expr.atom.IntervalExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ArithmeticOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.BitOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.IntervalQualifiers;
import com.aliyun.fastmodel.core.tree.expr.enums.IsType;
import com.aliyun.fastmodel.core.tree.expr.enums.LikeOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.LogicalOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DateLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLiteral;
import com.aliyun.fastmodel.core.tree.expr.similar.BetweenPredicate;
import com.aliyun.fastmodel.core.tree.expr.similar.LikePredicate;
import com.aliyun.fastmodel.core.tree.expr.similar.NotExpression;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.TableElement;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DefaultValueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexExpr;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexSortKey;
import com.aliyun.fastmodel.core.tree.statement.table.index.SortType;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.Algorithm;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.CheckExpressionConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.ForeignKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.ForeignKeyConstraint.MatchAction;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.ForeignKeyConstraint.ReferenceAction;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.ForeignKeyConstraint.ReferenceOperator;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.UniqueKeyExprConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ListPartitionValue;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionValue;
import com.aliyun.fastmodel.transform.oceanbase.format.OceanBasePropertyKey;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Binary_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Bit_exprContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Bit_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Blob_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Bool_priContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Bool_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Character_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Column_attributeContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Column_definitionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Column_definition_refContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Column_nameContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Column_refContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Complex_func_exprContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Complex_string_literalContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Constraint_nameContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Create_table_stmtContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Cur_timestamp_funcContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Data_typeContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Data_type_precisionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Date_year_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Datetime_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.ExprContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Float_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Geo_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Hash_partition_elementContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Hash_partition_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Hash_subpartition_elementContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Index_nameContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Index_using_algorithmContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Int_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Json_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Key_partition_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.List_partition_elementContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.List_partition_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.List_subpartition_elementContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.LiteralContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Normal_relation_factorContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Now_or_signed_literalContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Number_literalContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Number_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Opt_column_attribute_listContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Opt_column_partition_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Opt_constraint_nameContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Opt_hash_subpartition_listContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Opt_list_subpartition_listContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Opt_range_subpartition_listContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Out_of_line_constraintContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Out_of_line_indexContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Out_of_line_unique_indexContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Parallel_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Partition_attributes_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Precision_decimal_numContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Precision_int_numContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.PredicateContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Range_exprContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Range_partition_elementContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Range_partition_exprContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Range_partition_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Range_subpartition_elementContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Reference_actionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Reference_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.References_clauseContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Relation_factorContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Relation_nameContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Relation_name_or_stringContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Simple_func_exprContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Sort_column_keyContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Sql_stmtContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Stmt_listContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.String_length_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.String_val_listContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Subpartition_individual_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Subpartition_template_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Table_optionContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Table_option_listContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParser.Text_type_iContext;
import com.aliyun.fastmodel.transform.oceanbase.mysql.parser.OBParserBaseVisitor;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.OceanBaseMysqlCharDataType;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.OceanBaseMysqlCharDataType.CharsetKey;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.OceanBaseMysqlDataTypeName;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.OceanBaseMysqlGenericDataType;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.OceanBaseMysqlGenericDataType.SignEnum;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseHashPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseKeyPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseListPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseRangePartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.BaseSubPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubHashPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubKeyPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubListPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubRangePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.HashPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.ListPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.RangePartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubHashPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubListPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubPartitionList;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubRangePartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubHashTemplatePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubKeyTemplatePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubListTemplatePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubRangeTemplatePartition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * OceanBaseAstBuilder
 *
 * @author panguanjing
 * @date 2024/2/2
 */
public class OceanBaseMysqlAstBuilder extends OBParserBaseVisitor<Node> {

    private final ReverseContext reverseContext;

    public OceanBaseMysqlAstBuilder(ReverseContext reverseContext) {
        this.reverseContext = reverseContext;
    }

    @Override
    public Node visitSql_stmt(Sql_stmtContext ctx) {
        return visit(ctx.stmt_list());
    }

    @Override
    public Node visitStmt_list(Stmt_listContext ctx) {
        return visit(ctx.stmt());
    }

    @Override
    public Node visitCreate_table_stmt(Create_table_stmtContext ctx) {
        QualifiedName qualifiedName = (QualifiedName)visit(ctx.relation_factor());
        boolean isNotExist = ctx.EXISTS() != null && ctx.not() != null;
        List<TableElement> visit = ParserHelper.visit(this, ctx.table_element_list().table_element(), TableElement.class);
        List<ColumnDefinition> columnDefinitions = visit.stream().filter(
            t -> t instanceof ColumnDefinition
        ).map(t -> {
            return (ColumnDefinition)t;
        }).collect(Collectors.toList());

        //
        List<BaseConstraint> constraints = visit.stream().filter(
            t -> t instanceof BaseConstraint
        ).map(t -> (BaseConstraint)t).collect(Collectors.toList());

        PartitionedBy partitionedBy = toPartitionBy(ctx);
        List<Property> properties = buildProperties(ctx);
        List<TableIndex> tableIndexList = visit.stream().filter(
            t -> t instanceof TableIndex
        ).map(t -> (TableIndex)t).collect(Collectors.toList());
        return CreateTable.builder()
            .ifNotExist(isNotExist)
            .columns(columnDefinitions)
            .tableName(qualifiedName)
            .constraints(constraints)
            .partition(partitionedBy)
            .properties(properties)
            .comment(getComment(properties))
            .tableIndex(tableIndexList)
            .build();
    }

    @Override
    public Node visitOut_of_line_index(Out_of_line_indexContext ctx) {
        Identifier indexName = null;
        if (ctx.index_name() != null) {
            indexName = (Identifier)visit(ctx.index_name());
        }
        List<IndexSortKey> indexColumnNames = null;
        if (ctx.sort_column_list() != null) {
            indexColumnNames = ParserHelper.visit(this, ctx.sort_column_list().sort_column_key(), IndexSortKey.class);
        }
        List<Property> property = null;
        if (ctx.opt_index_options() != null) {
            property = ParserHelper.visit(this, ctx.opt_index_options().index_option(), Property.class);
        }
        TableIndex tableIndex = new TableIndex(
            indexName, indexColumnNames, property
        );
        return tableIndex;
    }

    @Override
    public Node visitIndex_name(Index_nameContext ctx) {
        return visit(ctx.relation_name());
    }

    @Override
    public Node visitSort_column_key(Sort_column_keyContext ctx) {
        Identifier columnName = null;
        if (ctx.column_name() != null) {
            columnName = (Identifier)visit(ctx.column_name());
        }
        LongLiteral columnLength = null;
        SortType sortType = null;
        if (ctx.DESC() != null) {
            sortType = SortType.DESC;
        }
        if (ctx.ASC() != null) {
            sortType = SortType.ASC;
        }
        if (columnName != null) {
            if (ctx.LeftParen() != null && ctx.INTNUM() != null) {
                columnLength = new LongLiteral(ctx.INTNUM(0).getText());
            }
            return new IndexColumnName(columnName, columnLength, sortType);
        } else {
            BaseExpression baseExpression = (BaseExpression)visit(ctx.expr());
            return new IndexExpr(baseExpression, sortType);
        }
    }

    private Comment getComment(List<Property> properties) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        Optional<Property> property = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(p.getName(), OceanBasePropertyKey.COMMENT.getValue())
        ).findFirst();
        if (property.isEmpty()) {
            return null;
        }
        return new Comment(property.get().getValue());
    }

    @Override
    public Node visitRelation_factor(Relation_factorContext ctx) {
        if (ctx.normal_relation_factor() != null) {
            return visit(ctx.normal_relation_factor());
        }
        return visit(ctx.dot_relation_factor());
    }

    @Override
    public Node visitRelation_name(Relation_nameContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitNormal_relation_factor(Normal_relation_factorContext ctx) {
        List<Identifier> list = ParserHelper.visit(this, ctx.relation_name(), Identifier.class);
        return QualifiedName.of(list);
    }

    @Override
    public Node visitColumn_name(Column_nameContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitColumn_definition(Column_definitionContext ctx) {
        Identifier colName = (Identifier)visit(ctx.column_definition_ref());
        BaseDataType baseDataType = (BaseDataType)visit(ctx.data_type());
        ListNode node = null;
        if (ctx.opt_column_attribute_list() != null) {
            node = (ListNode)visit(ctx.opt_column_attribute_list());
        }
        List<Property> properties = toColumnProperty(node);
        return ColumnDefinition.builder()
            .colName(colName)
            .dataType(baseDataType)
            .comment(toComment(node))
            .notNull(toNotNull(node))
            .primary(toPrimary(node))
            .defaultValue(toDefaultValue(node))
            .properties(properties)
            .build();
    }

    private List<Property> toColumnProperty(ListNode ctx) {
        return ParserHelper.getListNode(ctx, Property.class);
    }

    private Comment toComment(ListNode optColumnAttributeListContext) {
        if (optColumnAttributeListContext == null) {
            return null;
        }
        Comment comment = ParserHelper.getNode(optColumnAttributeListContext, Comment.class);
        return comment;
    }

    @Override
    public Node visitColumn_definition_ref(Column_definition_refContext ctx) {
        return visit(ctx.column_name());
    }

    @Override
    public Node visitOut_of_line_constraint(Out_of_line_constraintContext ctx) {
        Identifier constraintName = null;
        if (ctx.opt_constraint_name() != null) {
            constraintName = (Identifier)visit(ctx.opt_constraint_name());
        } else {
            constraintName = IdentifierUtil.sysIdentifier();
        }
        //primary key
        if (ctx.out_of_line_primary_index() != null) {
            List<Identifier> colNames = ParserHelper.visit(this,
                ctx.out_of_line_primary_index().column_name_list().column_name(), Identifier.class);
            PrimaryConstraint primaryConstraint = new PrimaryConstraint(
                constraintName,
                colNames
            );
            return primaryConstraint;
        }
        //unique key
        Out_of_line_unique_indexContext outOfLineUniqueIndexContext = ctx.out_of_line_unique_index();
        if (outOfLineUniqueIndexContext != null) {
            List<IndexSortKey> sortKeys = ParserHelper.visit(this,
                outOfLineUniqueIndexContext.sort_column_list().sort_column_key(), IndexSortKey.class);
            Algorithm algorithm = null;
            Identifier indexName = null;
            Index_nameContext indexNameContext = outOfLineUniqueIndexContext.index_name();
            if (indexNameContext != null) {
                indexName = (Identifier)visit(indexNameContext);
            }
            Index_using_algorithmContext indexUsingAlgorithmContext = outOfLineUniqueIndexContext.index_using_algorithm();
            if (indexUsingAlgorithmContext != null) {
                if (indexUsingAlgorithmContext.BTREE() != null) {
                    algorithm = Algorithm.BTREE;
                }
                if (indexUsingAlgorithmContext.HASH() != null) {
                    algorithm = Algorithm.HASH;
                }
            }
            UniqueKeyExprConstraint uniqueConstraint = new UniqueKeyExprConstraint(
                constraintName, indexName, sortKeys, algorithm
            );
            return uniqueConstraint;
        }
        //check constraint
        if (ctx.CHECK() != null) {
            BaseExpression expression = null;
            if (ctx.expr() != null) {
                expression = (BaseExpression)visit(ctx.expr());
            }
            Boolean enforced = null;
            if (ctx.check_state() != null) {
                enforced = ctx.check_state().NOT() == null;
            }
            return new CheckExpressionConstraint(constraintName, expression, enforced);
        }

        //foreign reference
        if (ctx.FOREIGN() != null) {
            return getForeignKeyConstraint(ctx, constraintName);
        }
        return super.visitOut_of_line_constraint(ctx);
    }

    private ForeignKeyConstraint getForeignKeyConstraint(Out_of_line_constraintContext ctx, Identifier constraintName) {
        List<Identifier> colNames = null;
        if (ctx.column_name_list() != null) {
            colNames = ParserHelper.visit(this, ctx.column_name_list().column_name(), Identifier.class);
        }
        QualifiedName referenceTable = null;
        Identifier indexName = null;
        if (ctx.index_name() != null) {
            indexName = (Identifier)visit(ctx.index_name());
        }
        List<Identifier> referenceColNames = null;
        References_clauseContext referencesClauseContext = ctx.references_clause();
        MatchAction matchAction = null;
        ReferenceAction referenceAction = null;
        ReferenceOperator referenceOperator = null;
        referenceTable = (QualifiedName)visit(referencesClauseContext.relation_factor());
        referenceColNames = ParserHelper.visit(this, referencesClauseContext.column_name_list().column_name(), Identifier.class);
        if (referencesClauseContext.match_action() != null) {
            matchAction = MatchAction.valueOf(referencesClauseContext.match_action().getText().toUpperCase());
        }
        Reference_optionContext referenceOptionContext = referencesClauseContext.reference_option();
        if (referenceOptionContext != null) {
            if (referenceOptionContext.UPDATE() != null) {
                referenceOperator = ReferenceOperator.UPDATE;
            } else if (referenceOptionContext.DELETE() != null) {
                referenceOperator = ReferenceOperator.DELETE;
            }
            Reference_actionContext referenceActionContext = referenceOptionContext.reference_action();
            referenceAction = getAction(referenceActionContext);
        }
        return new ForeignKeyConstraint(constraintName, indexName, colNames, referenceTable, referenceColNames, matchAction, referenceOperator,
            referenceAction);
    }

    private ReferenceAction getAction(Reference_actionContext referenceActionContext) {
        if (referenceActionContext == null) {
            return null;
        }
        if (referenceActionContext.RESTRICT() != null) {
            return ReferenceAction.RESTRICT;
        }
        if (referenceActionContext.CASCADE() != null) {
            return ReferenceAction.CASCADE;
        }
        if (referenceActionContext.NO() != null && referenceActionContext.ACTION() != null) {
            return ReferenceAction.NO_ACTION;
        }
        if (referenceActionContext.SET() != null && referenceActionContext.DEFAULT() != null) {
            return ReferenceAction.SET_DEFAULT;
        }
        if (referenceActionContext.SET() != null && referenceActionContext.NULLX() != null) {
            return ReferenceAction.SET_NULL;
        }
        return null;
    }

    @Override
    public Node visitData_type(Data_typeContext ctx) {
        return super.visitData_type(ctx);
    }

    @Override
    public Node visitInt_type_i(Int_type_iContext ctx) {
        Identifier identifier = null;
        if (ctx.BIGINT() != null) {
            identifier = new Identifier(ctx.BIGINT().getText());
        }
        if (ctx.INTEGER() != null) {
            identifier = new Identifier(ctx.INTEGER().getText());
        }

        if (ctx.TINYINT() != null) {
            identifier = new Identifier(ctx.TINYINT().getText());
        }

        if (ctx.SMALLINT() != null) {
            identifier = new Identifier(ctx.SMALLINT().getText());
        }

        if (ctx.MEDIUMINT() != null) {
            identifier = new Identifier(ctx.MEDIUMINT().getText());
        }

        List<DataTypeParameter> arguments = getDataTypeParameters(ctx.precision_int_num());

        Boolean zeroFill = null;
        if (ctx.ZEROFILL() != null) {
            zeroFill = true;
        }
        SignEnum signEnum = null;
        if (ctx.UNSIGNED() != null) {
            signEnum = SignEnum.UNSIGNED;
        } else if (ctx.SIGNED() != null) {
            signEnum = SignEnum.SIGNED;
        }
        return new OceanBaseMysqlGenericDataType(
            identifier, arguments,
            zeroFill, signEnum
        );
    }

    private List<DataTypeParameter> getDataTypeParameters(Precision_int_numContext ctx) {
        List<DataTypeParameter> arguments = Lists.newArrayList();
        if (ctx == null) {
            return arguments;
        }
        for (TerminalNode terminalNode : ctx.INTNUM()) {
            NumericParameter numericParameter = new NumericParameter(terminalNode.getText());
            arguments.add(numericParameter);
        }
        return arguments;
    }

    @Override
    public Node visitFloat_type_i(Float_type_iContext ctx) {
        String name = null;
        if (ctx.FLOAT() != null) {
            name = ctx.FLOAT().getText();
        }
        if (ctx.DOUBLE() != null) {
            if (ctx.PRECISION() != null) {
                name = OceanBaseMysqlDataTypeName.DOUBLE_PRECISION.getValue();
            } else {
                name = OceanBaseMysqlDataTypeName.DOUBLE.getValue();
            }
        }
        Boolean zeroFill = ctx.ZEROFILL() != null;
        SignEnum signEnum = null;
        if (ctx.UNSIGNED() != null) {
            signEnum = SignEnum.UNSIGNED;
        } else if (ctx.SIGNED() != null) {
            signEnum = SignEnum.SIGNED;
        }

        List<DataTypeParameter> dataTypeParameters = null;

        if (ctx.precision_int_num() != null) {
            dataTypeParameters = getDataTypeParameters(ctx.precision_int_num());
        }
        if (ctx.data_type_precision() != null) {
            dataTypeParameters = getDataTypeParameterByPrecision(ctx.data_type_precision());
        }
        return new OceanBaseMysqlGenericDataType(
            name, dataTypeParameters, zeroFill, signEnum
        );
    }

    private List<DataTypeParameter> getDataTypeParameterByPrecision(Data_type_precisionContext dataTypePrecisionContext) {
        if (dataTypePrecisionContext.precision_int_num() != null) {
            return getDataTypeParameters(dataTypePrecisionContext.precision_int_num());
        }
        Precision_decimal_numContext precisionDecimalNumContext = dataTypePrecisionContext.precision_decimal_num();
        NumericParameter numericParameter = new NumericParameter(precisionDecimalNumContext.DECIMAL_VAL().getText());
        return Lists.newArrayList(numericParameter);
    }

    @Override
    public Node visitNumber_type_i(Number_type_iContext ctx) {
        String name = null;
        if (ctx.DECIMAL() != null) {
            name = ctx.DECIMAL().getText();
        }
        if (ctx.NUMBER() != null) {
            name = ctx.NUMBER().getText();
        }
        if (ctx.FIXED() != null) {
            name = ctx.FIXED().getText();
        }
        List<DataTypeParameter> list = getDataTypeParameters(ctx.precision_int_num());
        Boolean zeroFill = ctx.ZEROFILL() != null;
        SignEnum signEnum = null;
        if (ctx.UNSIGNED() != null) {
            signEnum = SignEnum.UNSIGNED;
        }
        if (ctx.SIGNED() != null) {
            signEnum = SignEnum.SIGNED;
        }
        return new OceanBaseMysqlGenericDataType(name, list, zeroFill, signEnum);
    }

    @Override
    public Node visitBool_type_i(Bool_type_iContext ctx) {
        if (ctx.BOOL() != null) {
            return new OceanBaseMysqlGenericDataType(ctx.BOOL().getText(), null, null, null);
        }
        if (ctx.BOOLEAN() != null) {
            return new OceanBaseMysqlGenericDataType(ctx.BOOLEAN().getText(), null, null, null);
        }
        return null;
    }

    @Override
    public Node visitJson_type_i(Json_type_iContext ctx) {
        return new OceanBaseMysqlGenericDataType(ctx.JSON().getText(), null, null, null);
    }

    @Override
    public Node visitBit_type_i(Bit_type_iContext ctx) {
        List<DataTypeParameter> list = getDataTypeParameters(ctx.precision_int_num());
        return new OceanBaseMysqlGenericDataType(ctx.BIT().getText(), list, null, null);
    }

    @Override
    public Node visitText_type_i(Text_type_iContext ctx) {
        String name = null;
        if (ctx.TINYTEXT() != null) {
            name = ctx.TINYTEXT().getText();
        }
        if (ctx.TEXT() != null) {
            name = ctx.TEXT().getText();
        }
        if (ctx.MEDIUMTEXT() != null) {
            name = ctx.MEDIUMTEXT().getText();
        }
        if (ctx.LONGTEXT() != null) {
            name = ctx.LONGTEXT().getText();
        }
        List<DataTypeParameter> list = null;
        if (ctx.string_length_i() != null) {
            NumericParameter numericParameter = new NumericParameter(ctx.string_length_i().number_literal().getText());
            list.add(numericParameter);
        }
        return new OceanBaseMysqlGenericDataType(name, list, null, null);
    }

    @Override
    public Node visitCharacter_type_i(Character_type_iContext ctx) {
        String name = null;
        if (ctx.CHARACTER() != null) {
            name = ctx.CHARACTER().getText();
        }
        if (ctx.NCHAR() != null) {
            name = ctx.NCHAR().getText();
        }
        if (ctx.VARCHAR() != null) {
            name = ctx.VARCHAR().getText();
        }

        if (ctx.NVARCHAR() != null) {
            name = ctx.NVARCHAR().getText();
        }
        List<DataTypeParameter> list = getDataTypeFromStringLength(ctx.string_length_i());
        CharsetKey charSet = null;
        if (ctx.charset_key() != null) {
            charSet = ctx.charset_key().CHARACTER() != null ? CharsetKey.CHARACTER_SET : CharsetKey.CHARSET;
        }
        String charsetName = null;
        if (ctx.charset_name() != null) {
            charsetName = ctx.charset_name().getText();
        }
        String collation = null;
        if (ctx.collation() != null) {
            collation = StripUtils.strip(ctx.collation().collation_name().getText());
        }
        return new OceanBaseMysqlCharDataType(name, list, charSet, charsetName, collation);
    }

    private static List<DataTypeParameter> getDataTypeFromStringLength(String_length_iContext ctx) {
        List<DataTypeParameter> list = Lists.newArrayList();
        if (ctx == null) {
            return list;
        }
        NumericParameter numericParameter = new NumericParameter(ctx.number_literal().getText());
        list.add(numericParameter);
        return list;
    }

    @Override
    public Node visitGeo_type_i(Geo_type_iContext ctx) {
        return new OceanBaseMysqlGenericDataType(ctx.getText(), null, null, null);
    }

    @Override
    public Node visitDatetime_type_i(Datetime_type_iContext ctx) {
        String name = null;
        if (ctx.DATETIME() != null) {
            name = ctx.DATETIME().getText();
        }
        if (ctx.TIME() != null) {
            name = ctx.TIME().getText();
        }
        if (ctx.TIMESTAMP() != null) {
            name = ctx.TIMESTAMP().getText();
        }
        List<DataTypeParameter> list = getDataTypeParameters(ctx.precision_int_num());
        return new OceanBaseMysqlGenericDataType(name, list, null, null);
    }

    @Override
    public Node visitDate_year_type_i(Date_year_type_iContext ctx) {
        String name = null;
        if (ctx.DATE() != null) {
            name = ctx.DATE().getText();
        }
        if (ctx.YEAR() != null) {
            name = ctx.YEAR().getText();
        }
        List<DataTypeParameter> list = getDataTypeParameters(ctx.precision_int_num());
        return new OceanBaseMysqlGenericDataType(name, list, null, null);
    }

    @Override
    public Node visitBlob_type_i(Blob_type_iContext ctx) {
        String name = null;
        if (ctx.BLOB() != null) {
            name = ctx.BLOB().getText();
        }
        if (ctx.MEDIUMBLOB() != null) {
            name = ctx.MEDIUMBLOB().getText();
        }
        if (ctx.TINYBLOB() != null) {
            name = ctx.TINYBLOB().getText();
        }
        if (ctx.LONGBLOB() != null) {
            name = ctx.LONGBLOB().getText();
        }
        List<DataTypeParameter> list = getDataTypeFromStringLength(ctx.string_length_i());
        return new OceanBaseMysqlGenericDataType(name, list, null, null);
    }

    @Override
    public Node visitBinary_type_i(Binary_type_iContext ctx) {
        String name = null;
        if (ctx.BINARY() != null) {
            name = ctx.BINARY().getText();
        }
        if (ctx.VARBINARY() != null) {
            name = ctx.VARBINARY().getText();
        }
        List<DataTypeParameter> list = getDataTypeFromStringLength(ctx.string_length_i());
        return new OceanBaseMysqlGenericDataType(name, list, null, null);
    }

    @Override
    public Node visitOpt_column_attribute_list(Opt_column_attribute_listContext ctx) {
        List<Node> list = Lists.newArrayList();
        if (ctx.opt_column_attribute_list() != null) {
            int childCount = ctx.opt_column_attribute_list().getChildCount();
            for (int i = 0; i < childCount; i++) {
                Node node = visit(ctx.opt_column_attribute_list().getChild(i));
                list.add(node);
            }
        }
        Column_attributeContext columnAttributeContext = ctx.column_attribute();
        if (columnAttributeContext != null) {
            Node node = visit(columnAttributeContext);
            list.add(node);
        }
        return new ListNode(list);
    }

    @Override
    public Node visitColumn_attribute(Column_attributeContext ctx) {
        //not null
        if (ctx.not() != null && ctx.NULLX() != null) {
            return new NotNullConstraint(IdentifierUtil.sysIdentifier());
        }
        //comment
        if (ctx.COMMENT() != null) {
            TerminalNode terminalNode = ctx.STRING_VALUE();
            return new Comment(StripUtils.strip(terminalNode.getText()));
        }
        //check
        if (ctx.CHECK() != null) {
            Identifier identifier = IdentifierUtil.sysIdentifier();
            if (ctx.opt_constraint_name() != null) {
                identifier = (Identifier)visit(ctx.opt_constraint_name());
            }
            BaseExpression expression = (BaseExpression)visit(ctx.expr());
            Boolean enforced = null;
            if (ctx.check_state() != null) {
                enforced = ctx.check_state().NOT() == null && ctx.check_state().ENFORCED() != null;
            }
            return new CheckExpressionConstraint(identifier, expression, enforced);
        }
        //COLLATE
        if (ctx.COLLATE() != null) {
            String collateName = StripUtils.strip(ctx.collation_name().getText());
            return new Property(OceanBasePropertyKey.COLLATE.getValue(), collateName);
        }
        //default
        if (ctx.DEFAULT() != null) {
            BaseExpression baseExpression = (BaseExpression)visit(ctx.now_or_signed_literal());
            return new DefaultValueConstraint(IdentifierUtil.sysIdentifier(), baseExpression);
        }

        //unique key
        if (ctx.UNIQUE() != null && ctx.KEY() != null) {
            return new UniqueConstraint(IdentifierUtil.sysIdentifier(), Lists.newArrayList());
        }
        //primary key
        if (ctx.PRIMARY() != null || ctx.KEY() != null) {
            return new PrimaryConstraint(IdentifierUtil.sysIdentifier(), Lists.newArrayList());
        }
        //auto_increment
        if (ctx.AUTO_INCREMENT() != null) {
            return new Property(ExtensionPropertyKey.COLUMN_AUTO_INCREMENT.getValue(), String.valueOf(Boolean.TRUE));
        }
        //return null
        if (ctx.NULLX() != null) {
            return new NotNullConstraint(IdentifierUtil.sysIdentifier(), false);
        }
        return super.visitColumn_attribute(ctx);
    }

    @Override
    public Node visitOpt_constraint_name(Opt_constraint_nameContext ctx) {
        return visit(ctx.constraint_name());
    }

    @Override
    public Node visitConstraint_name(Constraint_nameContext ctx) {
        return visit(ctx.relation_name());
    }

    private BaseExpression toDefaultValue(ListNode optColumnAttributeListContext) {
        if (optColumnAttributeListContext == null) {
            return null;
        }
        DefaultValueConstraint defaultValueConstraint = ParserHelper.getNode(optColumnAttributeListContext, DefaultValueConstraint.class);
        if (defaultValueConstraint == null) {
            return null;
        }
        return defaultValueConstraint.getValue();
    }

    @Override
    public Node visitNow_or_signed_literal(Now_or_signed_literalContext ctx) {
        if (ctx.signed_literal() != null) {
            return visit(ctx.signed_literal());
        }
        return visit(ctx.cur_timestamp_func());
    }

    @Override
    public Node visitCur_timestamp_func(Cur_timestamp_funcContext ctx) {
        String text = ctx.now_synonyms_func().getText();
        List<BaseExpression> args = null;
        if (ctx.INTNUM() != null) {
            args = Lists.newArrayList();
            LongLiteral longLiteral = new LongLiteral(ctx.INTNUM().getText());
            args.add(longLiteral);
        }
        return new FunctionCall(QualifiedName.of(text), false, args);
    }

    @Override
    public Node visitLiteral(LiteralContext ctx) {
        if (ctx.BOOL_VALUE() != null) {
            return new BooleanLiteral(ctx.BOOL_VALUE().getText());
        }
        if (ctx.DATE_VALUE() != null) {
            String value = ctx.DATE_VALUE().getText();
            return parseDateValue(value);
        }
        if (ctx.INTNUM() != null) {
            return new LongLiteral(ctx.INTNUM().getText());
        }
        if (ctx.DECIMAL_VAL() != null) {
            return new DecimalLiteral(ctx.DECIMAL_VAL().getText());
        }
        if (ctx.NULLX() != null) {
            return new NullLiteral();
        }
        return super.visitLiteral(ctx);
    }

    @Override
    public Node visitComplex_string_literal(Complex_string_literalContext ctx) {
        if (ctx.STRING_VALUE() != null && ctx.string_val_list() == null) {
            String value = StripUtils.strip(ctx.STRING_VALUE().getText());
            return new StringLiteral(value);
        }
        if (ctx.string_val_list() != null) {
            String value = StripUtils.strip(ctx.STRING_VALUE().getText());
            StringLiteral stringLiteral = new StringLiteral(value);
            List<StringLiteral> list = ctx.string_val_list().STRING_VALUE().stream().map(x -> {
                String text = x.getText();
                return new StringLiteral(StripUtils.strip(text));
            }).collect(Collectors.toList());
            List<StringLiteral> all = Lists.newArrayList(stringLiteral);
            all.addAll(list);
            return new ListStringLiteral(all);
        }
        return super.visitComplex_string_literal(ctx);
    }

    private Node parseDateValue(String value) {
        if (StringUtils.startsWithIgnoreCase(value, "DATE")) {
            String substring = StringUtils.substring(value, "DATE".length());
            return new DateLiteral(substring);
        }
        if (StringUtils.startsWithIgnoreCase(value, "TIMESTAMP")) {
            String substring = StringUtils.substring(value, "TIMESTAMP".length());
            return new TimestampLiteral(substring);
        }
        return null;
    }

    @Override
    public Node visitNumber_literal(Number_literalContext ctx) {
        if (ctx.INTNUM() != null) {
            return new LongLiteral(ctx.INTNUM().getText());
        }
        if (ctx.DECIMAL_VAL() != null) {
            return new DecimalLiteral(ctx.DECIMAL_VAL().getText());
        }
        return super.visitNumber_literal(ctx);
    }

    private Boolean toPrimary(ListNode optColumnAttributeListContext) {
        if (optColumnAttributeListContext == null) {
            return null;
        }
        PrimaryConstraint primaryConstraint = ParserHelper.getNode(optColumnAttributeListContext, PrimaryConstraint.class);
        if (primaryConstraint == null) {
            return null;
        }
        return primaryConstraint.getEnable();
    }

    private Boolean toNotNull(ListNode optColumnAttributeListContext) {
        if (optColumnAttributeListContext == null) {
            return null;
        }
        NotNullConstraint visit = ParserHelper.getNode(optColumnAttributeListContext, NotNullConstraint.class);
        if (visit == null) {
            return null;
        }
        return visit.getEnable();
    }

    private PartitionedBy toPartitionBy(Create_table_stmtContext ctx) {
        if (ctx.opt_partition_option() == null) {
            return null;
        }
        return (PartitionedBy)visit(ctx.opt_partition_option());
    }

    @Override
    public Node visitOpt_column_partition_option(Opt_column_partition_optionContext ctx) {
        return super.visitOpt_column_partition_option(ctx);
    }

    @Override
    public Node visitHash_partition_option(Hash_partition_optionContext ctx) {
        LongLiteral partitionCount = null;
        if (ctx.INTNUM() != null) {
            partitionCount = new LongLiteral(ctx.INTNUM().getText());
        }
        BaseExpression expression = (BaseExpression)visit(ctx.expr());
        BaseSubPartition subPartition = null;
        if (ctx.subpartition_option() != null) {
            subPartition = (BaseSubPartition)visit(ctx.subpartition_option());
        }
        List<HashPartitionElement> hashPartitionElements = null;
        if (ctx.opt_hash_partition_list() != null) {
            hashPartitionElements = ParserHelper.visit(this, ctx.opt_hash_partition_list().hash_partition_list().hash_partition_element(),
                HashPartitionElement.class);
        }
        OceanBaseHashPartitionBy oceanBaseHashPartitionBy = new OceanBaseHashPartitionBy(
            partitionCount, expression, subPartition, hashPartitionElements
        );
        return oceanBaseHashPartitionBy;
    }

    @Override
    public Node visitList_partition_option(List_partition_optionContext ctx) {
        List<ColumnDefinition> columns = null;
        if (ctx.COLUMNS() != null) {
            columns = ParserHelper.visit(this, ctx.column_name_list().column_name(), Identifier.class).stream()
                .map(c -> {
                    return ColumnDefinition.builder().colName(c).build();
                }).collect(Collectors.toList());
        }
        BaseExpression expression = null;
        if (ctx.expr() != null) {
            expression = (BaseExpression)visit(ctx.expr());
        }
        LongLiteral partitionCount = null;
        if (ctx.INTNUM() != null) {
            partitionCount = new LongLiteral(ctx.INTNUM().getText());
        }
        BaseSubPartition subPartition = null;
        if (ctx.subpartition_option() != null) {
            subPartition = (BaseSubPartition)visit(ctx.subpartition_option());
        }

        List<ListPartitionElement> listPartitionElements = null;
        if (ctx.opt_list_partition_list() != null) {
            listPartitionElements = ParserHelper.visit(this, ctx.opt_list_partition_list().list_partition_list().list_partition_element(),
                ListPartitionElement.class);
        }
        OceanBaseListPartitionBy oceanBaseListPartitionBy = new OceanBaseListPartitionBy(
            columns, expression, partitionCount, subPartition, listPartitionElements
        );
        return oceanBaseListPartitionBy;
    }

    @Override
    public Node visitKey_partition_option(Key_partition_optionContext ctx) {
        List<ColumnDefinition> columns = null;
        if (ctx.column_name_list() != null) {
            columns = ParserHelper.visit(this, ctx.column_name_list().column_name(), Identifier.class)
                .stream().map(
                    c -> ColumnDefinition.builder().colName(c).build()
                ).collect(Collectors.toList());
        }
        LongLiteral partitionCount = getLongLiteral(ctx);
        BaseSubPartition subPartition = null;
        if (ctx.subpartition_option() != null) {
            subPartition = (BaseSubPartition)visit(ctx.subpartition_option());
        }

        List<HashPartitionElement> hashPartitionElements = null;
        if (ctx.opt_hash_partition_list() != null) {
            hashPartitionElements = ParserHelper.visit(this, ctx.opt_hash_partition_list().hash_partition_list().hash_partition_element(),
                HashPartitionElement.class);
        }
        return new OceanBaseKeyPartitionBy(
            columns, partitionCount, subPartition, hashPartitionElements
        );
    }

    @Override
    public Node visitColumn_ref(Column_refContext ctx) {
        QualifiedName qualifiedName = null;
        if (ctx.column_name() != null) {
            Identifier col = (Identifier)visit(ctx.column_name());
            qualifiedName = QualifiedName.of(col.getValue());
        } else {
            qualifiedName = QualifiedName.of(ctx.getText());
        }
        return new TableOrColumn(qualifiedName);
    }

    @Override
    public Node visitSubpartition_individual_option(Subpartition_individual_optionContext ctx) {
        BaseExpression expression = null;
        if (ctx.expr() != null) {
            expression = (BaseExpression)visit(ctx.expr());
        }
        List<Identifier> columns = null;
        if (ctx.column_name_list() != null) {
            columns = ParserHelper.visit(this, ctx.column_name_list().column_name(), Identifier.class)
                .stream()
                .collect(Collectors.toList());
        }
        LongLiteral count = getPartitionCount(ctx.INTNUM());
        if (ctx.RANGE() != null) {
            return new SubRangePartition(expression, columns, null);
        }
        if (ctx.BISON_LIST() != null) {
            return new SubListPartition(expression, columns);
        }
        if (ctx.HASH() != null) {
            return new SubHashPartition(expression, count);
        }
        if (ctx.KEY() != null) {
            return new SubKeyPartition(columns, count);
        }
        return super.visitSubpartition_individual_option(ctx);
    }

    private static LongLiteral getLongLiteral(Key_partition_optionContext ctx) {
        LongLiteral partitionCount = null;
        if (ctx.INTNUM() != null) {
            partitionCount = new LongLiteral(ctx.INTNUM().getText());
        }
        return partitionCount;
    }

    @Override
    public Node visitRange_partition_option(Range_partition_optionContext ctx) {
        List<ColumnDefinition> columns = null;
        if (ctx.COLUMNS() != null) {
            columns = ParserHelper.visit(this, ctx.column_name_list().column_name(), Identifier.class)
                .stream()
                .map(c -> ColumnDefinition.builder().colName(c).build())
                .collect(Collectors.toList());
        }
        LongLiteral partitionCount = getPartitionCount(ctx.INTNUM());
        BaseSubPartition subPartition = null;
        if (ctx.subpartition_option() != null) {
            subPartition = (BaseSubPartition)visit(ctx.subpartition_option());
        }
        BaseExpression expression = null;
        if (ctx.expr() != null) {
            expression = (BaseExpression)visit(ctx.expr());
        }
        List<RangePartitionElement> singleRangePartitionList = null;
        if (ctx.opt_range_partition_list() != null) {
            singleRangePartitionList = ParserHelper.visit(this,
                ctx.opt_range_partition_list().range_partition_list().range_partition_element(), RangePartitionElement.class);
        }
        return new OceanBaseRangePartitionBy(
            columns,
            partitionCount,
            subPartition,
            expression,
            singleRangePartitionList
        );
    }

    @Override
    public Node visitSubpartition_template_option(Subpartition_template_optionContext ctx) {
        BaseExpression expression = null;
        if (ctx.expr() != null) {
            expression = (BaseExpression)visit(ctx.expr());
        }
        List<Identifier> columnList = null;
        if (ctx.COLUMNS() != null) {
            columnList = ParserHelper.visit(this, ctx.column_name_list().column_name(), Identifier.class);
        }
        List<SubPartitionElement> elements = null;
        if (ctx.opt_hash_subpartition_list() != null) {
            elements = ParserHelper.visit(this, ctx.opt_hash_subpartition_list().hash_subpartition_list().hash_subpartition_element(),
                SubPartitionElement.class);
        }
        if (ctx.RANGE() != null) {
            // SUBPARTITION BY RANGE LeftParen expr RightParen SUBPARTITION TEMPLATE opt_range_subpartition_list
            //    | SUBPARTITION BY RANGE COLUMNS LeftParen column_name_list RightParen SUBPARTITION TEMPLATE opt_range_subpartition_list

            SubPartitionList subPartitionList = null;
            List<SubPartitionElement> partitionList = null;
            if (ctx.opt_range_subpartition_list() != null) {
                partitionList = ParserHelper.visit(this, ctx.opt_range_subpartition_list().range_subpartition_list().range_subpartition_element(),
                    SubPartitionElement.class);
                subPartitionList = new SubPartitionList(partitionList);
            }
            return new SubRangeTemplatePartition(expression, columnList, subPartitionList);
        }
        if (ctx.HASH() != null) {
            SubPartitionList subPartitionList = null;
            if (elements != null) {
                subPartitionList = new SubPartitionList(elements);
            }
            return new SubHashTemplatePartition(expression, subPartitionList);
        }

        if (ctx.BISON_LIST() != null) {
            SubPartitionList subPartitionList = null;
            List<SubPartitionElement> listSubPartitionElements = null;
            if (ctx.opt_list_subpartition_list() != null) {
                listSubPartitionElements = ParserHelper.visit(this,
                    ctx.opt_list_subpartition_list().list_subpartition_list().list_subpartition_element(),
                    SubPartitionElement.class);
                subPartitionList = new SubPartitionList(listSubPartitionElements);
            }
            return new SubListTemplatePartition(expression, columnList, subPartitionList);
        }

        if (ctx.KEY() != null) {
            SubPartitionList subPartitionList = null;
            if (elements != null) {
                subPartitionList = new SubPartitionList(elements);
            }
            return new SubKeyTemplatePartition(columnList, subPartitionList);
        }
        return super.visitSubpartition_template_option(ctx);
    }

    @Override
    public Node visitRange_partition_element(Range_partition_elementContext ctx) {
        QualifiedName name = (QualifiedName)visit(ctx.relation_factor());
        PartitionKey partitionKey = null;
        if (ctx.range_partition_expr() != null) {
            partitionKey = (PartitionKey)visit(ctx.range_partition_expr());
        }
        List<Property> propertyList = null;
        if (ctx.partition_attributes_option() != null) {
            Property property = (Property)visit(ctx.partition_attributes_option());
            propertyList = Lists.newArrayList();
            propertyList.add(property);
        }
        LongLiteral longLiteral = getPartitionCount(ctx.INTNUM());
        SubPartitionList subPartitionList = null;
        if (ctx.subpartition_list() != null) {
            subPartitionList = (SubPartitionList)visit(ctx.subpartition_list());
        }
        Identifier identifier = name.getOriginalParts().get(name.getOriginalParts().size() - 1);
        SingleRangePartition singleRangePartition = new SingleRangePartition(
            identifier, false,
            partitionKey, propertyList
        );
        return new RangePartitionElement(longLiteral, singleRangePartition, subPartitionList);
    }

    @Override
    public Node visitOpt_hash_subpartition_list(Opt_hash_subpartition_listContext ctx) {
        List<SubPartitionElement> list = ParserHelper.visit(this,
            ctx.hash_subpartition_list().hash_subpartition_element(),
            SubPartitionElement.class
        );
        return new SubPartitionList(list);
    }

    @Override
    public Node visitHash_subpartition_element(Hash_subpartition_elementContext ctx) {
        QualifiedName qualifiedName = (QualifiedName)visit(ctx.relation_factor());
        Property property = null;
        if (ctx.partition_attributes_option() != null) {
            property = (Property)visit(ctx.partition_attributes_option());
        }
        return new SubHashPartitionElement(qualifiedName, property);
    }

    @Override
    public Node visitOpt_range_subpartition_list(Opt_range_subpartition_listContext ctx) {
        List<SubPartitionElement> list = ParserHelper.visit(this, ctx.range_subpartition_list().range_subpartition_element(),
            SubPartitionElement.class);
        return new SubPartitionList(list);
    }

    @Override
    public Node visitRange_subpartition_element(Range_subpartition_elementContext ctx) {
        Identifier name = null;
        if (ctx.relation_factor() != null) {
            QualifiedName qualifiedName = (QualifiedName)visit(ctx.relation_factor());
            name = qualifiedName.getOriginalParts().get(qualifiedName.getOriginalParts().size() - 1);
        }
        PartitionKey partitionKey = null;
        if (ctx.range_partition_expr() != null) {
            partitionKey = (PartitionKey)visit(ctx.range_partition_expr());
        }

        List<Property> propertyList = null;
        if (ctx.partition_attributes_option() != null) {
            propertyList = Lists.newArrayList();
            Property property = (Property)visit(ctx.partition_attributes_option());
            propertyList.add(property);
        }
        SingleRangePartition singleRangePartition = new SingleRangePartition(
            name, false, partitionKey, propertyList
        );
        return new SubRangePartitionElement(singleRangePartition);
    }

    @Override
    public Node visitList_subpartition_element(List_subpartition_elementContext ctx) {
        QualifiedName qualifiedName = (QualifiedName)visit(ctx.relation_factor());
        Boolean defaultExpr = null;
        List<BaseExpression> expressionList = null;
        if (ctx.list_partition_expr() != null) {
            if (ctx.list_partition_expr().DEFAULT() != null) {
                defaultExpr = true;
            } else {
                expressionList = ParserHelper.visit(this, ctx.list_partition_expr().list_expr().expr(), BaseExpression.class);
            }
        }
        Property property = null;
        if (ctx.partition_attributes_option() != null) {
            property = (Property)visit(ctx.partition_attributes_option());
        }
        return new SubListPartitionElement(qualifiedName, defaultExpr, expressionList, property);
    }

    @Override
    public Node visitHash_partition_element(Hash_partition_elementContext ctx) {
        QualifiedName qualifiedName = (QualifiedName)visit(ctx.relation_factor());
        LongLiteral num = getPartitionCount(ctx.INTNUM());
        Property property = null;
        if (ctx.partition_attributes_option() != null) {
            property = (Property)visit(ctx.partition_attributes_option());
        }
        SubPartitionList subPartitionList = null;
        if (ctx.subpartition_list() != null) {
            subPartitionList = (SubPartitionList)visit(ctx.subpartition_list());
        }
        return new HashPartitionElement(qualifiedName, num, property, subPartitionList);
    }

    @Override
    public Node visitList_partition_element(List_partition_elementContext ctx) {
        QualifiedName qualifiedName = (QualifiedName)visit(ctx.relation_factor());
        Boolean defaultExpr = null;
        List<BaseExpression> expressionList = null;
        if (ctx.list_partition_expr().DEFAULT() != null) {
            defaultExpr = true;
        } else {
            expressionList = ParserHelper.visit(this, ctx.list_partition_expr().list_expr().expr(), BaseExpression.class);
        }
        LongLiteral num = null;
        if (ctx.INTNUM() != null) {
            num = getPartitionCount(ctx.INTNUM());
        }
        Property property = null;
        if (ctx.partition_attributes_option() != null) {
            property = (Property)visit(ctx.partition_attributes_option());
        }
        SubPartitionList subPartitionList = null;
        if (ctx.subpartition_list() != null) {
            subPartitionList = (SubPartitionList)visit(ctx.subpartition_list());
        }
        return new ListPartitionElement(qualifiedName, defaultExpr, expressionList, num, property, subPartitionList);
    }

    @Override
    public Node visitOpt_list_subpartition_list(Opt_list_subpartition_listContext ctx) {
        List<SubPartitionElement> elementList = ParserHelper.visit(this, ctx.list_subpartition_list().list_subpartition_element(),
            SubPartitionElement.class);
        return new SubPartitionList(elementList);
    }

    @Override
    public Node visitPartition_attributes_option(Partition_attributes_optionContext ctx) {
        return new Property(ctx.ENGINE_().getText(), ctx.INNODB().getText());
    }

    @Override
    public Node visitRange_partition_expr(Range_partition_exprContext ctx) {
        boolean maxValue = ctx.MAXVALUE() != null;
        ListPartitionValue partitionValues = null;
        if (ctx.range_expr_list() != null) {
            List<PartitionValue> valueList = ParserHelper.visit(this, ctx.range_expr_list().range_expr(), PartitionValue.class);
            partitionValues = new ListPartitionValue(valueList);
        }
        return new LessThanPartitionKey(maxValue, partitionValues);
    }

    @Override
    public Node visitRange_expr(Range_exprContext ctx) {
        boolean maxValue = ctx.MAXVALUE() != null;
        BaseExpression stringLiteral = null;
        if (ctx.expr() != null) {
            stringLiteral = (BaseExpression)visit(ctx.expr());
        }
        return new PartitionValue(maxValue, stringLiteral);
    }

    @Override
    public Node visitExpr(ExprContext ctx) {
        if (ctx.NOT() != null) {
            BaseExpression baseExpression = (BaseExpression)visit(ctx.expr(0));
            return new NotExpression(ParserHelper.getLocation(ctx), baseExpression);
        }
        if (ctx.LeftParen() != null) {
            BaseExpression baseExpression = (BaseExpression)visit(ctx.expr(0));
            baseExpression.setParenthesized(true);
            return baseExpression;
        }
        if (ctx.bool_pri() != null) {
            BaseExpression baseExpression = (BaseExpression)visit(ctx.bool_pri());
            if (ctx.IS() != null) {
                return getIsConditionExpression(ctx, null, baseExpression);
            }
            return baseExpression;
        }
        BaseExpression left = (BaseExpression)visit(ctx.expr(0));
        BaseExpression right = (BaseExpression)visit(ctx.expr(1));
        LogicalOperator operator = getOperator(ctx);
        return new LogicalBinaryExpression(operator, left, right);
    }

    private static IsConditionExpression getIsConditionExpression(ExprContext ctx, IsType isType, BaseExpression baseExpression) {
        TerminalNode terminalNode = ctx.BOOL_VALUE();
        if (ctx.not() != null) {
            if (StringUtils.equalsIgnoreCase(terminalNode.getText(), "TRUE")) {
                isType = IsType.NOT_TRUE;
            } else if (StringUtils.equalsIgnoreCase(terminalNode.getText(), "FALSE")) {
                isType = IsType.NOT_FALSE;
            }
        } else {
            if (StringUtils.equalsIgnoreCase(terminalNode.getText(), "TRUE")) {
                isType = IsType.TRUE;
            } else if (StringUtils.equalsIgnoreCase(terminalNode.getText(), "FALSE")) {
                isType = IsType.FALSE;
            }
        }
        return new IsConditionExpression(baseExpression, isType);
    }

    @Override
    public Node visitBool_pri(Bool_priContext ctx) {
        ComparisonOperator comparisonOperator = getComparisonOperator(ctx);
        if (comparisonOperator != null && ctx.predicate() != null) {
            BaseExpression left = (BaseExpression)visit(ctx.bool_pri());
            BaseExpression right = (BaseExpression)visit(ctx.predicate());
            return new ComparisonExpression(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), comparisonOperator, left, right);
        }
        return super.visitBool_pri(ctx);
    }

    private ComparisonOperator getComparisonOperator(Bool_priContext ctx) {
        if (ctx.COMP_EQ() != null) {
            return ComparisonOperator.EQUAL;
        }
        if (ctx.COMP_GT() != null) {
            return ComparisonOperator.GREATER_THAN;
        }
        if (ctx.COMP_GE() != null) {
            return ComparisonOperator.GREATER_THAN_OR_EQUAL;
        }
        if (ctx.COMP_LE() != null) {
            return ComparisonOperator.LESS_THAN_OR_EQUAL;
        }
        if (ctx.COMP_LT() != null) {
            return ComparisonOperator.LESS_THAN;
        }
        if (ctx.COMP_NE() != null) {
            return ComparisonOperator.NOT_EQUAL_MS;
        }
        if (ctx.COMP_NSEQ() != null) {
            return ComparisonOperator.NS_EQUAL;
        }
        return null;
    }

    @Override
    public Node visitPredicate(PredicateContext ctx) {
        //in
        if (ctx.IN() != null) {
            InPredicate inPredicate = null;
            BaseExpression baseExpression = (BaseExpression)visit(ctx.bit_expr(0));
            BaseExpression valueList = (BaseExpression)visit(ctx.in_expr());
            inPredicate = new InPredicate(baseExpression, valueList);
            if (ctx.not() != null) {
                return new NotExpression(ParserHelper.getLocation(ctx), inPredicate);
            } else {
                return inPredicate;
            }
        }
        //between and
        if (ctx.BETWEEN() != null) {
            BaseExpression value = (BaseExpression)visit(ctx.bit_expr(0));
            BaseExpression min = (BaseExpression)visit(ctx.bit_expr(1));
            BaseExpression max = (BaseExpression)visit(ctx.predicate());
            BetweenPredicate betweenPredicate = new BetweenPredicate(value, min, max);
            if (ctx.not() != null) {
                return new NotExpression(ParserHelper.getLocation(ctx), betweenPredicate);
            }
            return betweenPredicate;
        }
        //like
        if (ctx.LIKE() != null) {
            BaseExpression left = (BaseExpression)visit(ctx.bit_expr(0));
            LikeOperator operator = LikeOperator.LIKE;
            BaseExpression target = null;
            if (CollectionUtils.isNotEmpty(ctx.simple_expr())) {
                target = (BaseExpression)visit(ctx.simple_expr(0));
            } else {
                target = (BaseExpression)visit(ctx.string_val_list(0));
            }
            LikePredicate likePredicate = new LikePredicate(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), left, operator, null,
                target);
            if (ctx.not() != null) {
                return new NotExpression(ParserHelper.getLocation(ctx), likePredicate);
            } else {
                return likePredicate;
            }
        }
        //REGEXP
        if (ctx.REGEXP() != null) {
            LikeOperator operator = LikeOperator.REGEXP;
            BaseExpression left = (BaseExpression)visit(ctx.bit_expr(0));
            BaseExpression target = null;
            if (ctx.bit_expr().size() > 1) {
                target = (BaseExpression)visit(ctx.bit_expr(1));
            } else {
                target = (BaseExpression)visit(ctx.string_val_list(0));
            }
            LikePredicate likePredicate = new LikePredicate(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), left, operator, null,
                target);
            if (ctx.not() != null) {
                return new NotExpression(ParserHelper.getLocation(ctx), likePredicate);
            } else {
                return likePredicate;
            }
        }
        return super.visitPredicate(ctx);
    }

    @Override
    public Node visitString_val_list(String_val_listContext ctx) {
        List<StringLiteral> stringLiteralList = ctx.STRING_VALUE().stream().map(
            s -> {
                String text = s.getText();
                String t = StripUtils.strip(text);
                return new StringLiteral(t);
            }
        ).collect(Collectors.toList());
        return new ListStringLiteral(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), stringLiteralList);
    }

    @Override
    public Node visitBit_expr(Bit_exprContext ctx) {
        if (ctx.simple_expr() != null) {
            return visit(ctx.simple_expr());
        }

        if (ctx.INTERVAL() != null) {
            BaseLiteral intervalValue = (BaseLiteral)visit(ctx.bit_expr(0));
            IntervalQualifiers intervalQualifiers = IntervalQualifiers.getIntervalQualifiers(ctx.date_unit().getText());
            BaseExpression baseExpression = (BaseExpression)visit(ctx.expr());
            return new IntervalExpression(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx),
                intervalValue, intervalQualifiers, baseExpression, null);
        }

        BitOperator operator = getBitOperator(ctx);
        if (operator != null) {
            BaseExpression left = (BaseExpression)visit(ctx.bit_expr(0));
            BaseExpression right = (BaseExpression)visit(ctx.bit_expr(1));
            return new BitOperationExpression(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), operator, left, right);
        }

        ArithmeticOperator arithmeticOperator = getArithmeticOperator(ctx);
        if (arithmeticOperator != null) {
            BaseExpression left = (BaseExpression)visit(ctx.bit_expr(0));
            BaseExpression right = (BaseExpression)visit(ctx.bit_expr(1));
            return new ArithmeticBinaryExpression(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), arithmeticOperator, left, right);
        }

        return null;
    }

    private ArithmeticOperator getArithmeticOperator(Bit_exprContext ctx) {
        if (ctx.DIV() != null || ctx.Div() != null) {
            return ArithmeticOperator.DIVIDE;
        }
        if (ctx.MOD() != null || ctx.Mod() != null) {
            return ArithmeticOperator.MOD;
        }
        if (ctx.Plus() != null) {
            return ArithmeticOperator.ADD;
        }
        if (ctx.Star() != null) {
            return ArithmeticOperator.STAR;
        }
        if (ctx.Minus() != null) {
            return ArithmeticOperator.SUBTRACT;
        }
        return null;
    }

    private BitOperator getBitOperator(Bit_exprContext ctx) {
        if (ctx.And() != null) {
            return BitOperator.AMPERSAND;
        }
        if (ctx.Caret() != null) {
            return BitOperator.BITWISE_XOR;
        }

        if (ctx.Or() != null) {
            return BitOperator.BITWISE;
        }

        if (ctx.SHIFT_LEFT() != null) {
            return BitOperator.SHIFT_LEFT;
        }

        if (ctx.SHIFT_RIGHT() != null) {
            return BitOperator.SHIFT_RIGHT;
        }
        return null;
    }

    private LogicalOperator getOperator(ExprContext ctx) {
        if (ctx.AND() != null || ctx.AND_OP() != null) {
            return LogicalOperator.AND;
        }
        if (ctx.OR() != null || ctx.CNNOP() != null) {
            return LogicalOperator.OR;
        }
        return null;
    }

    private LongLiteral getPartitionCount(TerminalNode intnumText) {
        if (intnumText == null) {
            return null;
        }
        return new LongLiteral(intnumText.getText());
    }

    private List<Property> buildProperties(Create_table_stmtContext ctx) {
        Table_option_listContext tableOptionListContext = ctx.table_option_list();
        if (tableOptionListContext == null) {
            return ImmutableList.of();
        }
        if (tableOptionListContext.table_option_list_space_seperated() != null) {
            List<Property> list = ParserHelper.visit(this, tableOptionListContext.table_option_list_space_seperated().table_option(),
                Property.class);
            return list;
        }
        return null;
    }

    @Override
    public Node visitTable_option(Table_optionContext ctx) {
        if (ctx.SORTKEY() != null) {
            List<Identifier> list = ParserHelper.visit(this, ctx.column_name_list().column_name(), Identifier.class);
            String cols = list.stream().map(Identifier::getValue).collect(Collectors.joining(","));
            return new Property(OceanBasePropertyKey.SORT_KEY.getValue(), cols);
        }
        if (ctx.TABLE_MODE() != null) {
            TerminalNode terminalNode = ctx.STRING_VALUE();
            return new Property(OceanBasePropertyKey.TABLE_MODE.getValue(), StripUtils.strip(terminalNode.getText()));
        }
        if (ctx.COMMENT() != null) {
            TerminalNode terminalNode = ctx.STRING_VALUE();
            return new Property(OceanBasePropertyKey.COMMENT.getValue(), StripUtils.strip(terminalNode.getText()));
        }
        if (ctx.COMPRESSION() != null) {
            TerminalNode terminalNode = ctx.STRING_VALUE();
            return new Property(OceanBasePropertyKey.COMPRESSION.getValue(), StripUtils.strip(terminalNode.getText()));
        }
        if (ctx.charset_key() != null) {
            String value = StripUtils.strip(ctx.charset_name().getText());
            return new Property(OceanBasePropertyKey.CHARSET_KEY.getValue(), value);
        }
        if (ctx.collation_name() != null) {
            String value = StripUtils.strip(ctx.collation_name().getText());
            return new Property(OceanBasePropertyKey.COLLATE.getValue(), value);
        }

        if (ctx.ROW_FORMAT() != null) {
            return new Property(OceanBasePropertyKey.ROW_FORMAT.getValue(), StripUtils.strip(ctx.row_format_option().getText()));
        }
        if (ctx.PCTFREE() != null) {
            return new Property(OceanBasePropertyKey.PCTFREE.getValue(), ctx.INTNUM().getText());
        }
        if (ctx.USE_BLOOM_FILTER() != null) {
            return new Property(OceanBasePropertyKey.USE_BLOOM_FILTER.getValue(), ctx.BOOL_VALUE().getText());
        }
        if (ctx.REPLICA_NUM() != null) {
            return new Property(OceanBasePropertyKey.REPLICA_NUM.getValue(), ctx.INTNUM().getText());
        }
        if (ctx.TABLET_SIZE() != null) {
            return new Property(OceanBasePropertyKey.TABLET_SIZE.getValue(), ctx.INTNUM().getText());
        }
        if (ctx.BLOCK_SIZE() != null) {
            return new Property(OceanBasePropertyKey.BLOCK_SIZE.getValue(), ctx.INTNUM().getText());
        }
        if (ctx.STORAGE_FORMAT_VERSION() != null) {
            return new Property(OceanBasePropertyKey.STORAGE_FORMAT_VERSION.getValue(), ctx.INTNUM().getText());
        }
        if (ctx.PROGRESSIVE_MERGE_NUM() != null) {
            return new Property(OceanBasePropertyKey.PROGRESSIVE_MERGE_NUM.getValue(), ctx.INTNUM().getText());
        }
        if (ctx.MAX_USED_PART_ID() != null) {
            return new Property(OceanBasePropertyKey.MAX_USED_PART_ID.getValue(), ctx.INTNUM().getText());
        }
        if (ctx.ENGINE_() != null) {
            Node node = visit(ctx.relation_name_or_string());
            if (node instanceof StringLiteral) {
                StringLiteral stringLiteral = (StringLiteral)node;
                return new Property(ExtensionPropertyKey.TABLE_ENGINE.getValue(), stringLiteral.getValue());
            } else if (node instanceof Identifier) {
                Identifier identifier = (Identifier)node;
                return new Property(ExtensionPropertyKey.TABLE_ENGINE.getValue(), identifier.getValue());
            }
        }
        return super.visitTable_option(ctx);
    }

    @Override
    public Node visitRelation_name_or_string(Relation_name_or_stringContext ctx) {
        if (ctx.relation_name() != null) {
            return visit(ctx.relation_name());
        }
        if (ctx.STRING_VALUE() != null) {
            String v = StripUtils.strip(ctx.STRING_VALUE().getText());
            return new StringLiteral(v);
        }
        if (ctx.ALL() != null) {
            return new Identifier(ctx.ALL().getText());
        }
        return super.visitRelation_name_or_string(ctx);
    }

    @Override
    public Node visitSimple_func_expr(Simple_func_exprContext ctx) {
        QualifiedName functionName = null;
        if (ctx.function_name() != null) {
            functionName = QualifiedName.of(ctx.function_name().getText());
        } else if (ctx.func_name != null) {
            String text = ctx.func_name.getText();
            functionName = QualifiedName.of(text);
        }
        List<BaseExpression> arguments = null;
        if (ctx.expr() != null) {
            arguments = ParserHelper.visit(this, ctx.expr(), BaseExpression.class);
        } else if (ctx.expr_list() != null) {
            arguments = ParserHelper.visit(this, ctx.expr_list().expr(), BaseExpression.class);
        }
        return new FunctionCall(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx),
            functionName,
            ctx.DISTINCT() != null, arguments
        );
    }

    @Override
    public Node visitComplex_func_expr(Complex_func_exprContext ctx) {
        if (ctx.CAST() != null) {
            BaseExpression expression = (BaseExpression)visit(ctx.expr().expr(0));
            BaseDataType dataType = (BaseDataType)visit(ctx.cast_data_type());
            return new Cast(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), expression, dataType);
        }
        return super.visitComplex_func_expr(ctx);
    }

    @Override
    public Node visitParallel_option(Parallel_optionContext ctx) {
        if (ctx.NOPARALLEL() != null) {
            return new Property(OceanBasePropertyKey.PARALLEL.getValue(), ctx.NOPARALLEL().getText());
        }
        return new Property(OceanBasePropertyKey.PARALLEL.getValue(), ctx.INTNUM().getText());
    }
}
