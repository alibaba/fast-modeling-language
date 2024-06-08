package com.aliyun.fastmodel.transform.spark.format;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.hive.format.HiveHelper;
import com.aliyun.fastmodel.transform.hive.format.HivePropertyKey;
import com.aliyun.fastmodel.transform.spark.context.SparkTableFormat;
import com.aliyun.fastmodel.transform.spark.context.SparkTransformContext;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.util.PropertyKeyUtil.getProperty;
import static java.util.stream.Collectors.joining;

/**
 * SparkVisitor
 * https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-hiveformat.html
 *
 * @author panguanjing
 * @date 2023/2/13
 */
public class SparkVisitor extends FastModelVisitor {

    private SparkTransformContext sparkTransformContext;
    private Map<SparkTableFormat, BiConsumer<CreateTable, String>> formatFunctions = Maps.newHashMap();

    public SparkVisitor(SparkTransformContext context) {
        this.sparkTransformContext = context == null ? SparkTransformContext.builder().build() : context;

        //init function
        BiConsumer<CreateTable, String> createTableStringBiConsumer = dataSourceFormat();
        formatFunctions.put(SparkTableFormat.DATASOURCE_FORMAT, createTableStringBiConsumer);
        BiConsumer<CreateTable, String> createTableHiveFormat = hiveFormat();
        formatFunctions.put(SparkTableFormat.HIVE_FORMAT, createTableHiveFormat);
    }

    /**
     * hive format
     *
     * @return
     */
    private BiConsumer<CreateTable, String> hiveFormat() {
        BiConsumer<CreateTable, String> hiveFormat = (node, elementIndent) -> {
            //append comment
            appendComment(node);
            //append partition
            if (!node.isPartitionEmpty()) {
                builder.append(
                    formatPartitions(
                        node.getPartitionedBy().getColumnDefinitions(),
                        isEndNewLine(builder.toString()),
                        elementIndent)
                );
            }
            //clustered by
            appendClusteredBy(node);
            //sorted by
            appendSortedBy(node);

            //append into buckets
            appendNumBuckets(node);

            //append row format
            String rowFormat = HiveHelper.appendRowFormat(node, elementIndent);
            if (StringUtils.isNotBlank(rowFormat)) {
                appendLineIfNecessary();
                builder.append(rowFormat);
            }
            //append stored format

            String storedFormat = HiveHelper.appendStoredFormat(node);
            if (StringUtils.isNotBlank(storedFormat)) {
                appendLineIfNecessary();
                builder.append(storedFormat);
            }
            //appendLocation
            String location = HiveHelper.appendLocation(node);
            if (StringUtils.isNotBlank(location)) {
                appendLineIfNecessary();
                builder.append(location);
            }

            //append tblProperties
            appendTblProperties(node);

        };
        return hiveFormat;
    }

    private void appendTblProperties(CreateTable node) {
        if (node.isPropertyEmpty()) {
            return;
        }
        List<Property> propertyList = node.getProperties().stream().filter(property -> {
            String name = property.getName();
            HivePropertyKey byValue = HivePropertyKey.getByValue(name);
            //一种是自定义的默认打印
            if (byValue == null) {
                SparkPropertyKey sparkPropertyKey = SparkPropertyKey.getByValue(name);
                if (sparkPropertyKey == null) {
                    return true;
                } else {
                    return sparkPropertyKey.isSupportPrint();
                }
            }
            //一种是系统自定义的属性，并且能够打印，
            return byValue.isSupportPrint();
        }).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(propertyList)) {
            return;
        }
        appendLineIfNecessary();
        builder.append("TBLPROPERTIES (");
        String collect = propertyList.stream().map(x ->
            formatStringLiteral(x.getName()) + "=" + formatStringLiteral(x.getValue())).collect(joining(","));
        builder.append(collect);
        builder.append(")");
    }

    /**
     * data source format
     *
     * @return
     */
    private BiConsumer<CreateTable, String> dataSourceFormat() {
        return (node, elementIndent) -> {
            String property = getProperty(node, SparkPropertyKey.USING);
            if (StringUtils.isNotBlank(property)) {
                appendLineIfNecessary();
                builder.append("USING ").append(property);
            }

            //append partition
            if (!node.isPartitionEmpty()) {
                builder.append(
                    formatPartitions(
                        node.getPartitionedBy().getColumnDefinitions(),
                        isEndNewLine(builder.toString()),
                        elementIndent)
                );
            }

            //clustered by
            appendClusteredBy(node);
            //sorted by
            appendSortedBy(node);

            //append into buckets
            appendNumBuckets(node);

            //appendLocation
            String location = HiveHelper.appendLocation(node);
            if (StringUtils.isNotBlank(location)) {
                appendLineIfNecessary();
                builder.append(location);
            }

            //append comment
            appendComment(node);

            //append tblProperties
            appendTblProperties(node);
        };
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnNotEmpty = node.getColumnDefines() != null && !node.getColumnDefines().isEmpty();
        boolean executable = true;
        if (!columnNotEmpty) {
            executable = false;
        }
        boolean external = HiveHelper.isExternal(node);
        if (external) {
            builder.append("CREATE EXTERNAL TABLE ");
        } else {
            builder.append("CREATE TABLE ");
        }
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = getCode(node.getQualifiedName());
        builder.append(tableName);

        int newIndent = indent + 1;
        String elementIndent = indentString(newIndent);
        appendColumns(node, columnNotEmpty, elementIndent);

        BiConsumer<CreateTable, String> function = formatFunctions.get(sparkTransformContext.getSparkTableFormat());
        function.accept(node, elementIndent);

        removeNewLine(builder);
        return executable;
    }

    private void appendColumns(CreateTable node, boolean columnNotEmpty, String elementIndent) {
        if (columnNotEmpty) {
            builder.append(newLine("("));
            String columnList = formatColumnList(node.getColumnDefines(), elementIndent);
            builder.append(columnList);
            builder.append(newLine(")"));
        } else {
            if (!node.isCommentElementEmpty()) {
                builder.append(newLine(COMMENT + "("));
                builder.append(formatCommentElement(node.getColumnCommentElements(), elementIndent));
                builder.append(newLine(COMMENT + ")"));
            }
        }
    }

    /**
     * append comment
     *
     * @param node
     */
    private void appendComment(CreateTable node) {
        if (node.getComment() != null) {
            appendLineIfNecessary();
            builder.append(formatComment(node.getComment(), isEndNewLine(builder.toString())));
        }
    }

    private void appendNumBuckets(CreateTable node) {
        String property = getProperty(node, SparkPropertyKey.NUMBER_BUCKETS);
        if (StringUtils.isBlank(property)) {
            return;
        }
        appendLineIfNecessary();
        builder.append("INTO ").append(property).append(" BUCKETS");
    }

    private void appendLineIfNecessary() {
        if (!isEndNewLine(builder.toString())) {
            builder.append(StringUtils.LF);
        }
    }

    private void appendSortedBy(CreateTable node) {
        String property = getProperty(node, SparkPropertyKey.SORTED_BY);
        if (StringUtils.isBlank(property)) {
            return;
        }
        appendLineIfNecessary();
        builder.append("SORTED BY ").append("(").append(property).append(")");
    }

    private void appendClusteredBy(CreateTable node) {
        String value = getProperty(node, SparkPropertyKey.CLUSTERED_BY);
        if (StringUtils.isBlank(value)) {
            return;
        }
        appendLineIfNecessary();
        builder.append("CLUSTERED BY ").append("(").append(value).append(")");
    }

}
