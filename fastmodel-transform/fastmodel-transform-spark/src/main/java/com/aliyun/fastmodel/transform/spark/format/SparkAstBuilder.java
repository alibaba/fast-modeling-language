package com.aliyun.fastmodel.transform.spark.format;

import java.util.List;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.CommentSpecContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.ComplexColTypeContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.ComplexDataTypeContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.CreateOrReplaceTableColTypeContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.CreateTableContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.CreateTableHeaderContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.DayTimeIntervalDataTypeContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.ErrorCapturingIdentifierContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.IdentifierContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.MultipartIdentifierContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.PrimitiveDataTypeContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.SingleStatementContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParser.StringLitContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkParserBaseVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.tree.TerminalNode;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;
import static com.aliyun.fastmodel.common.parser.ParserHelper.visitIfPresent;
import static java.util.stream.Collectors.toList;

/**
 * spark ast builder
 *
 * @author panguanjing
 * @date 2023/2/23
 */
public class SparkAstBuilder extends SparkParserBaseVisitor<Node> {

    private ReverseContext sparkTransformContext;

    public SparkAstBuilder(ReverseContext context) {
        this.sparkTransformContext = context;
    }

    @Override
    public Node visitSingleStatement(SingleStatementContext ctx) {
        return visit(ctx.statement());
    }



    @Override
    public Node visitCreateTable(CreateTableContext ctx) {
        CreateTableHeaderContext tableHeader = ctx.createTableHeader();
        QualifiedName tableName = (QualifiedName)visit(tableHeader.multipartIdentifier());
        boolean ifNotExists = tableHeader.IF() != null;
        List<ColumnDefinition> columns = ParserHelper.visit(this, ctx.createOrReplaceTableColTypeList().createOrReplaceTableColType(),
            ColumnDefinition.class);
        PartitionedBy partition = null;
        Comment comment = null;
        if (ctx.createTableClauses() != null) {
            if (ctx.createTableClauses().partitioning != null) {
                List<ColumnDefinition> list = ParserHelper.visit(this, ctx.createTableClauses().partitionFieldList(), ColumnDefinition.class);
                partition = new PartitionedBy(list);
            }
            if (ctx.createTableClauses().commentSpec() != null) {
                List<Comment> visit = ParserHelper.visit(this, ctx.createTableClauses().commentSpec(), Comment.class);
                comment = visit.get(0);
            }
        }
        List<Property> properties = Lists.newArrayList();
        return CreateTable.builder()
            .ifNotExist(ifNotExists)
            .tableName(tableName)
            .columns(columns)
            .partition(partition)
            .properties(properties)
            .comment(comment)
            .build();
    }

    private StringLiteral getStringLiteral(TerminalNode terminalNode) {
        return new StringLiteral(StripUtils.strip(terminalNode.getText()));
    }

    @Override
    public Node visitCommentSpec(CommentSpecContext ctx) {
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.stringLit());
        Comment comment = new Comment(stringLiteral.getValue());
        return comment;
    }

    @Override
    public Node visitMultipartIdentifier(MultipartIdentifierContext ctx) {
        List<Identifier> visit = ParserHelper.visit(this, ctx.errorCapturingIdentifier(), Identifier.class);
        return QualifiedName.of(visit);
    }

    @Override
    public Node visitComplexDataType(ComplexDataTypeContext ctx) {
        if (ctx.ARRAY() != null) {
            return new GenericDataType(
                getLocation(ctx),
                getOrigin(ctx),
                ctx.ARRAY().getText(),
                ImmutableList.of(new TypeParameter((BaseDataType)visit(ctx.dataType(0)))));
        }
        if (ctx.MAP() != null) {
            return new GenericDataType(
                getLocation(ctx),
                getOrigin(ctx),
                ctx.MAP().getText(),
                ImmutableList.of(
                    new TypeParameter((BaseDataType)visit(ctx.dataType(0))),
                    new TypeParameter((BaseDataType)visit(ctx.dataType(1)))));
        }
        if (ctx.STRUCT() != null) {
            List<Field> list = ParserHelper.visit(this, ctx.complexColTypeList().complexColType(), Field.class);
            return new RowDataType(list);
        }
        return null;
    }

    @Override
    public Node visitComplexColType(ComplexColTypeContext ctx) {
        Identifier name = (Identifier)visit(ctx.identifier());
        BaseDataType dataType = (BaseDataType)visit(ctx.dataType());
        Comment comment = visitIfPresent(this, ctx.commentSpec(), Comment.class).orElse(null);
        return new Field(getLocation(ctx), name, dataType, comment);
    }

    @Override
    public Node visitErrorCapturingIdentifier(ErrorCapturingIdentifierContext ctx) {
        return visit(ctx.identifier());
    }

    @Override
    public Node visitDayTimeIntervalDataType(DayTimeIntervalDataTypeContext ctx) {
        return super.visitDayTimeIntervalDataType(ctx);
    }

    @Override
    public Node visitPrimitiveDataType(PrimitiveDataTypeContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        List<DataTypeParameter> parameters = null;
        if (ctx.INTEGER_VALUE() != null) {
            parameters = ctx.INTEGER_VALUE().stream().map(i -> new NumericParameter(i.getText())).collect(toList());
        }
        return new GenericDataType(getLocation(ctx), getOrigin(ctx), identifier.getValue(), parameters);
    }

    @Override
    public Node visitCreateOrReplaceTableColType(CreateOrReplaceTableColTypeContext ctx) {
        Identifier colName = (Identifier)visit(ctx.colName);
        BaseDataType dataType = (BaseDataType)visit(ctx.dataType());
        Comment comment = null;
        if (ctx.colDefinitionOption() != null) {
            List<Node> node = ParserHelper.visit(this, ctx.colDefinitionOption(), Node.class);
            for (Node n : node) {
                if (n instanceof Comment) {
                    comment = (Comment)n;
                    continue;
                }
            }
        }
        return ColumnDefinition.builder()
            .colName(colName)
            .dataType(dataType)
            .comment(comment)
            .build();
    }

    @Override
    public Node visitIdentifier(IdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitStringLit(StringLitContext ctx) {
        TerminalNode string = ctx.STRING();
        if (string != null) {
            return getStringLiteral(string);
        }
        return getStringLiteral(ctx.DOUBLEQUOTED_STRING());
    }

}
