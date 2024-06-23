package com.aliyun.fastmodel.transform.doris.parser.visitor;

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
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentTimestamp;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.ClusterKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ListPartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.MultiRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ListPartitionValue;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionValue;
import com.aliyun.fastmodel.transform.doris.format.DorisPropertyKey;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.BooleanLiteralContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.ColumnDefContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.CommentSpecContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.ComplexColTypeContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.ComplexDataTypeContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.ConstantSeqContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.CreateTableContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.DataTypeContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.DecimalLiteralContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.ErrorCapturingIdentifierContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.FixedPartitionDefContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.IdentifierContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.IdentifierListContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.InPartitionDefContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.IntegerLiteralContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.LessThanPartitionDefContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.MultiStatementsContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.MultipartIdentifierContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.NullLiteralContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.PartitionDefContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.PartitionValueDefContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.PrimitiveColTypeContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.PrimitiveDataTypeContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.PropertyItemContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.PropertyKeyContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.PropertyValueContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.StepPartitionDefContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParser.StringLiteralContext;
import com.aliyun.fastmodel.transform.doris.parser.DorisParserBaseVisitor;
import com.aliyun.fastmodel.transform.doris.parser.tree.DorisDataTypeName;
import com.aliyun.fastmodel.transform.doris.parser.tree.DorisGenericDataType;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;
import static com.aliyun.fastmodel.common.parser.ParserHelper.visitIfPresent;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.COLUMN_AGG_DESC;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.COLUMN_AUTO_INCREMENT;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.COLUMN_KEY;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_ENGINE;

/**
 * DorisAstBuilder
 *
 * @author panguanjing
 * @date 2024/1/20
 */
public class DorisAstBuilder extends DorisParserBaseVisitor<Node> {

    private final ReverseContext context;

    public DorisAstBuilder(ReverseContext context) {
        this.context = context;
    }

    @Override
    public Node visitMultiStatements(MultiStatementsContext ctx) {
        List<BaseStatement> list = ParserHelper.visit(this, ctx.statement(), BaseStatement.class);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        if (list.size() == 1) {
            return list.get(0);
        }
        return new CompositeStatement(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), list);
    }

    @Override
    public Node visitCreateTable(CreateTableContext ctx) {
        boolean ifNotExist = ctx.NOT() != null && ctx.EXISTS() != null;
        QualifiedName tableName = (QualifiedName)visit(ctx.name);
        List<ColumnDefinition> list = null;
        List<TableIndex> tableIndices = null;
        Comment comment = null;
        if (ctx.ctasCols != null) {
        } else {
            list = ParserHelper.visit(this,
                ctx.columnDefs().columnDef(), ColumnDefinition.class);
            if (ctx.indexDefs() != null) {
                tableIndices = ParserHelper.visit(this, ctx.indexDefs().indexDef(), TableIndex.class);
            }
        }
        List<Property> properties = Lists.newArrayList();
        if (ctx.ENGINE() != null) {
            Property property = getEngineProp(ctx.engine);
            properties.add(property);
        }
        List<BaseConstraint> constraints = Lists.newArrayList();
        //aggregate, unique, duplicate
        if (ctx.AGGREGATE() != null) {
            AggregateKeyConstraint aggregateKeyConstraint = getAggregateKeyConstraint(ctx);
            constraints.add(aggregateKeyConstraint);
        }
        if (ctx.UNIQUE() != null) {
            UniqueConstraint uniqueConstraint = getUniqueConstraint(ctx);
            constraints.add(uniqueConstraint);
        }
        if (ctx.DUPLICATE() != null) {
            DuplicateKeyConstraint duplicateKeyConstraint = getDuplicateKeyConstraint(ctx);
            constraints.add(duplicateKeyConstraint);
        }
        //cluster by
        if (ctx.CLUSTER() != null) {
            ClusterKeyConstraint clusterByConstraint = getClusterByConstraint(ctx);
            constraints.add(clusterByConstraint);
        }

        //distributed by
        if (ctx.DISTRIBUTED() != null) {
            //distribute key constraint
            DistributeConstraint distributeKeyConstraint = getDistributeKeyConstraint(ctx);
            constraints.add(distributeKeyConstraint);
        }

        //table comment
        if (ctx.STRING_LITERAL() != null) {
            String value = StripUtils.strip(ctx.STRING_LITERAL().getText());
            comment = new Comment(value);
        }

        //partition by
        PartitionedBy partitionedBy = null;
        if (ctx.PARTITION() != null) {
            partitionedBy = getPartitionBy(ctx);
        }

        //table properties
        if (ctx.properties != null) {
            List<Property> propertyList = ParserHelper.visit(this,
                ctx.properties.propertyItemList().propertyItem(), Property.class);
            properties.addAll(propertyList);
        }

        return CreateTable.builder()
            .tableName(tableName)
            .columns(list)
            .properties(properties)
            .tableIndex(tableIndices)
            .constraints(constraints)
            .partition(partitionedBy)
            .comment(comment)
            .build();
    }

    private PartitionedBy getPartitionBy(CreateTableContext ctx) {
        List<PartitionDesc> list = null;
        List<ColumnDefinition> columnDefines = null;
        if (ctx.partitionKeys != null) {
            List<Identifier> columns = ParserHelper.visit(this,
                ctx.partitionKeys.identifierSeq().ident, Identifier.class);
            columnDefines = columns.stream().map(
                i -> ColumnDefinition.builder()
                    .colName(i)
                    .build()
            ).collect(Collectors.toList());
        }
        if (ctx.partitionsDef() != null) {
            list = ParserHelper.visit(this, ctx.partitionsDef().partitions, PartitionDesc.class);
        }
        if (ctx.RANGE() != null) {
            RangePartitionedBy rangePartitionedBy = new RangePartitionedBy(
                columnDefines, list
            );
            return rangePartitionedBy;
        }
        if (ctx.LIST() != null) {
            ListPartitionedBy listPartitionedBy = new ListPartitionedBy(columnDefines, list);
            return listPartitionedBy;
        }
        FunctionCall functionCall = null;
        if (ctx.partitionExpr != null) {
            functionCall = (FunctionCall)visit(ctx.partitionExpr);
            ExpressionPartitionBy expressionPartitionBy = new ExpressionPartitionBy(
                columnDefines, functionCall, list
            );
            return expressionPartitionBy;
        }
        return null;
    }

    @Override
    public Node visitPartitionDef(PartitionDefContext ctx) {
        List<Property> list = null;
        if (ctx.partitionProperties != null) {
            list = ParserHelper.visit(this, ctx.partitionProperties.propertyItem(), Property.class);
        }
        //singleRange Partition
        if (ctx.lessThanPartitionDef() != null) {
            LessThanPartitionDefContext lessThanPartitionDefContext = ctx.lessThanPartitionDef();
            SingleRangePartition singleRangePartition = new SingleRangePartition(
                (Identifier)visit(lessThanPartitionDefContext.partitionName),
                lessThanPartitionDefContext.IF() != null,
                (PartitionKey)visit(lessThanPartitionDefContext),
                list
            );
            return singleRangePartition;
        }

        //singleRange Partition
        FixedPartitionDefContext fixedPartitionDefContext = ctx.fixedPartitionDef();
        if (fixedPartitionDefContext != null) {
            SingleRangePartition singleRangePartition = new SingleRangePartition(
                (Identifier)visit(fixedPartitionDefContext.partitionName),
                fixedPartitionDefContext.IF() != null,
                (PartitionKey)visit(ctx.fixedPartitionDef()),
                list
            );
            return singleRangePartition;
        }

        return super.visitPartitionDef(ctx);
    }

    @Override
    public Node visitLessThanPartitionDef(LessThanPartitionDefContext ctx) {
        ListPartitionValue visit = (ListPartitionValue)visit(ctx.constantSeq());
        return new LessThanPartitionKey(
            ctx.MAXVALUE() != null,
            visit
        );
    }

    @Override
    public Node visitFixedPartitionDef(FixedPartitionDefContext ctx) {
        ListPartitionValue lower = (ListPartitionValue)visit(ctx.lower);
        ListPartitionValue upper = (ListPartitionValue)visit(ctx.upper);
        return new ArrayPartitionKey(Lists.newArrayList(lower, upper));
    }

    @Override
    public Node visitConstantSeq(ConstantSeqContext ctx) {
        List<PartitionValue> valueList = ParserHelper.visit(this,
            ctx.values, PartitionValue.class);
        return new ListPartitionValue(
            valueList
        );
    }

    @Override
    public Node visitPartitionValueDef(PartitionValueDefContext ctx) {

        BaseExpression baseExpression = null;
        if (ctx.INTEGER_VALUE() != null) {
            baseExpression = new LongLiteral(ctx.INTEGER_VALUE().getText());
        }
        if (ctx.STRING_LITERAL() != null) {
            String v = StripUtils.strip(ctx.STRING_LITERAL().getText());
            baseExpression = new StringLiteral(v);
        }
        boolean maxValue = false;
        if (ctx.MAXVALUE() != null) {
            maxValue = true;
        }
        PartitionValue partitionValue = new PartitionValue(
            maxValue, baseExpression
        );
        return partitionValue;
    }

    @Override
    public Node visitNullLiteral(NullLiteralContext ctx) {
        return new NullLiteral();
    }

    @Override
    public Node visitBooleanLiteral(BooleanLiteralContext ctx) {
        String value = ctx.booleanValue().getText();
        return new BooleanLiteral(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), value);
    }

    @Override
    public Node visitStringLiteral(StringLiteralContext ctx) {
        String value = StripUtils.strip(ctx.STRING_LITERAL().getText());
        return new StringLiteral(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), value);
    }

    @Override
    public Node visitIntegerLiteral(IntegerLiteralContext ctx) {
        String value = ctx.INTEGER_VALUE().getText();
        if (ctx.SUBTRACT() != null) {
            value = "-" + value;
        }
        return new LongLiteral(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), value);
    }

    @Override
    public Node visitDecimalLiteral(DecimalLiteralContext ctx) {
        String number = null;
        if (ctx.DECIMAL_VALUE() != null) {
            number = ctx.DECIMAL_VALUE().getText();
        } else if (ctx.EXPONENT_VALUE() != null) {
            number = ctx.EXPONENT_VALUE().getText();
        }
        if (ctx.SUBTRACT() != null) {
            number = "-" + number;
        }
        return new DecimalLiteral(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), number);
    }

    @Override
    public Node visitStepPartitionDef(StepPartitionDefContext ctx) {
        ListPartitionValue fromPartitionValue = (ListPartitionValue)visit(ctx.from);
        ListPartitionValue toPartitionValue = (ListPartitionValue)visit(ctx.to);
        LongLiteral longLiteral = new LongLiteral(ctx.unitsAmount.getText());
        DateTimeEnum timeEnum = null;
        if (ctx.datetimeUnit() != null) {
            timeEnum = DateTimeEnum.getByCode(ctx.datetimeUnit().getText());
        }
        IntervalLiteral intervalLiteral = new IntervalLiteral(
            longLiteral, timeEnum
        );
        return new MultiRangePartition(fromPartitionValue, toPartitionValue, intervalLiteral, null);
    }

    @Override
    public Node visitInPartitionDef(InPartitionDefContext ctx) {
        return super.visitInPartitionDef(ctx);
    }

    private DuplicateKeyConstraint getDuplicateKeyConstraint(CreateTableContext ctx) {
        List<Identifier> list = ParserHelper.visit(this, ctx.keys.identifierSeq().ident, Identifier.class);
        DuplicateKeyConstraint duplicateKeyConstraint = new DuplicateKeyConstraint(
            IdentifierUtil.sysIdentifier(),
            list
        );
        return duplicateKeyConstraint;
    }

    private UniqueConstraint getUniqueConstraint(CreateTableContext ctx) {
        List<Identifier> list = ParserHelper.visit(this, ctx.keys.identifierSeq().ident, Identifier.class);
        UniqueConstraint uniqueConstraint = new UniqueConstraint(
            IdentifierUtil.sysIdentifier(),
            list
        );
        return uniqueConstraint;
    }

    private AggregateKeyConstraint getAggregateKeyConstraint(CreateTableContext ctx) {
        List<Identifier> list = ParserHelper.visit(this, ctx.keys.identifierSeq().ident, Identifier.class);
        AggregateKeyConstraint aggregateKeyConstraint = new AggregateKeyConstraint(
            IdentifierUtil.sysIdentifier(),
            list
        );
        return aggregateKeyConstraint;
    }

    private ClusterKeyConstraint getClusterByConstraint(CreateTableContext ctx) {
        IdentifierListContext clusterKeys = ctx.clusterKeys;
        List<Identifier> list = ParserHelper.visit(this, ctx.clusterKeys.identifierSeq().ident, Identifier.class);
        ClusterKeyConstraint clusterByConstraint = new ClusterKeyConstraint(list);
        return clusterByConstraint;
    }

    private DistributeConstraint getDistributeKeyConstraint(CreateTableContext ctx) {
        IdentifierListContext hashKeys = ctx.hashKeys;
        List<Identifier> identifiers = ParserHelper.visit(this, hashKeys.identifierSeq().ident, Identifier.class);
        Boolean random = ctx.RANDOM() != null;
        Integer bucket = null;
        if (ctx.INTEGER_VALUE() != null) {
            bucket = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        }
        DistributeConstraint distributeKeyConstraint = new DistributeConstraint(
            identifiers, random, bucket
        );
        return distributeKeyConstraint;
    }

    private Property getEngineProp(IdentifierContext engine) {
        return new Property(TABLE_ENGINE.getValue(),
            engine.getText());
    }

    @Override
    public Node visitColumnDef(ColumnDefContext ctx) {
        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName((Identifier)visit(ctx.colName))
            .dataType((BaseDataType)visit(ctx.type))
            .defaultValue(getDefaultValue(ctx))
            .comment(getColComment(ctx))
            .notNull(getNotNull(ctx))
            .properties(getProperties(ctx))
            .build();
        return columnDefinition;
    }

    private List<Property> getProperties(ColumnDefContext ctx) {
        List<Property> properties = Lists.newArrayList();

        if (ctx.aggType != null) {
            Property property = new Property(COLUMN_AGG_DESC.getValue(), ctx.aggType.getText());
            properties.add(property);
        }
        if (ctx.KEY() != null) {
            Property property = new Property(COLUMN_KEY.getValue(), "KEY");
            properties.add(property);
        }

        if (ctx.AUTO_INCREMENT() != null) {
            Property property = new Property(COLUMN_AUTO_INCREMENT.getValue(), BooleanUtils.toStringTrueFalse(true));
            properties.add(property);
        }

        if (ctx.ON() != null && ctx.UPDATE() != null) {
            String value = CurrentTimestamp.CURRENT_TIMESTAMP;
            if (ctx.onUpdateValuePrecision != null) {
                value = value + "(" + ctx.onUpdateValuePrecision.getText() + ")";
            }
            Property property = new Property(DorisPropertyKey.COLUMN_ON_UPDATE_CURRENT_TIMESTAMP.getValue(), value);
            properties.add(property);
        }

        return properties;
    }

    private Comment getColComment(ColumnDefContext ctx) {
        if (ctx.comment == null) {
            return null;
        }
        String comment = StripUtils.strip(ctx.comment.getText());
        return new Comment(comment);
    }

    private Boolean getNotNull(ColumnDefContext ctx) {
        if (ctx.NOT() != null && ctx.NULL() != null) {
            return true;
        }
        if (ctx.NULL() != null) {
            return false;
        }
        return null;
    }

    private BaseExpression getDefaultValue(ColumnDefContext ctx) {
        if (ctx.DEFAULT() == null) {
            return null;
        }
        if (ctx.nullValue != null) {
            return new NullLiteral();
        }
        if (ctx.INTEGER_VALUE() != null) {
            return new LongLiteral(ctx.INTEGER_VALUE().getText());
        }
        if (ctx.stringValue != null) {
            String v = StripUtils.strip(ctx.stringValue.getText());
            return new StringLiteral(v);
        }
        if (ctx.CURRENT_TIMESTAMP() != null && ctx.ON() == null) {
            //TODO 需要支持函数中含有数字的方式
            return new CurrentTimestamp();
        }
        return null;
    }

    @Override
    public Node visitComplexDataType(ComplexDataTypeContext ctx) {
        if (ctx.ARRAY() != null) {
            List<DataTypeParameter> argument = Lists.newArrayList();
            DataTypeContext dataTypeContext = ctx.dataType(0);
            BaseDataType baseDataType = (BaseDataType)visit(dataTypeContext);
            argument.add(new TypeParameter(baseDataType));
            return new DorisGenericDataType(DorisDataTypeName.ARRAY.getValue(), argument);
        }
        if (ctx.MAP() != null) {
            List<DataTypeParameter> typeParameters = ctx.dataType().stream().map(
                d -> {
                    BaseDataType baseDataType = (BaseDataType)visit(d);
                    return new TypeParameter(baseDataType);
                }
            ).collect(Collectors.toList());
            return new DorisGenericDataType(DorisDataTypeName.Map.getValue(), typeParameters);
        }

        if (ctx.STRUCT() != null) {
            List<Field> list = ParserHelper.visit(this, ctx.complexColTypeList().complexColType(), Field.class);
            return new RowDataType(getLocation(ctx), getOrigin(ctx), list);
        }
        return super.visitComplexDataType(ctx);
    }

    @Override
    public Node visitComplexColType(ComplexColTypeContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        BaseDataType dataType = (BaseDataType)visit(ctx.dataType());
        Comment comment = visitIfPresent(this, ctx.commentSpec(), Comment.class).orElse(null);
        Field field = new Field(identifier, dataType, comment);
        return field;
    }

    @Override
    public Node visitCommentSpec(CommentSpecContext ctx) {
        String strip = StripUtils.strip(ctx.STRING_LITERAL().getText());
        return new Comment(strip);
    }

    @Override
    public Node visitPrimitiveDataType(PrimitiveDataTypeContext ctx) {
        PrimitiveColTypeContext primitiveColTypeContext = ctx.primitiveColType();
        String text = primitiveColTypeContext.type.getText();
        IDataTypeName dorisDataTypeName = null;
        if (primitiveColTypeContext.SIGNED() != null) {
            dorisDataTypeName = DorisDataTypeName.getByValue("SIGNED " + text);
        } else if (primitiveColTypeContext.UNSIGNED() != null) {
            dorisDataTypeName = DorisDataTypeName.getByValue("UNSIGNED " + text);
        } else {
            dorisDataTypeName = DorisDataTypeName.getByValue(text);
        }
        //类型参数
        List<DataTypeParameter> typeParameters = Lists.newArrayList();
        if (ctx.LEFT_PAREN() != null && ctx.RIGHT_PAREN() != null) {
            if (!ctx.INTEGER_VALUE().isEmpty()) {
                typeParameters = ctx.INTEGER_VALUE().stream().map(i -> {
                    DataTypeParameter dataTypeParameter = new NumericParameter(i.getText());
                    return dataTypeParameter;
                }).collect(Collectors.toList());
            }
        }
        DorisGenericDataType dorisGenericDataType = new DorisGenericDataType(
            ParserHelper.getLocation(ctx),
            ParserHelper.getOrigin(ctx),
            dorisDataTypeName.getValue(),
            typeParameters
        );
        return dorisGenericDataType;
    }

    @Override
    public Node visitMultipartIdentifier(MultipartIdentifierContext ctx) {
        List<Identifier> list = ParserHelper.visit(this,
            ctx.parts, Identifier.class);
        return QualifiedName.of(list);
    }

    @Override
    public Node visitErrorCapturingIdentifier(ErrorCapturingIdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitIdentifier(IdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitPropertyItem(PropertyItemContext ctx) {
        PropertyKeyContext key = ctx.key;
        String keyString = null;
        String valueString = null;
        if (key.constant() != null) {
            keyString = StripUtils.strip(key.constant().getText());
        } else if (key.identifier() != null) {
            Identifier identifier = (Identifier)visit(key.identifier());
            keyString = identifier.getValue();
        }
        PropertyValueContext value = ctx.value;
        if (value.constant() != null) {
            valueString = StripUtils.strip(value.constant().getText());
        } else if (value.identifier() != null) {
            Identifier identifier = (Identifier)visit(value.identifier());
            valueString = identifier.getValue();
        }
        return new Property(keyString, valueString);
    }
}
