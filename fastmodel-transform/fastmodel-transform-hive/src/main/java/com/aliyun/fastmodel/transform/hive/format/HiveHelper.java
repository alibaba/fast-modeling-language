package com.aliyun.fastmodel.transform.hive.format;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.util.PropertyKeyUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static java.util.stream.Collectors.joining;

/**
 * HiveHelper
 *
 * @author panguanjing
 * @date 2023/2/15
 */
public class HiveHelper {

    public static boolean isExternal(CreateTable node) {
        if (node.isPropertyEmpty()) {
            return false;
        }
        List<Property> properties = node.getProperties();
        Optional<Property> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(p.getName(), HivePropertyKey.EXTERNAL_TABLE.getValue())).findFirst();
        if (!first.isPresent()) {
            return false;
        }
        Property property = first.get();
        return StringUtils.equalsIgnoreCase(property.getValue(), BooleanUtils.toStringTrueFalse(true));
    }

    public static String appendRowFormat(CreateTable node, String elementIndent) {
        StringBuilder builder = new StringBuilder();
        List<Property> properties = node.getProperties();
        if (node.isPropertyEmpty()) {
            return StringUtils.EMPTY;
        }

        Optional<Property> serde = properties.stream().filter(
                p -> StringUtils.equalsIgnoreCase(p.getName(), HivePropertyKey.ROW_FORMAT_SERDE.getValue())).findFirst();
        if (serde.isPresent()) {
            builder.append("ROW FORMAT SERDE");
            String value = serde.get().getValue();
            builder.append(StringUtils.LF).append(String.format("'%s'", value));
        }

        Optional<Property> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(p.getName(), HivePropertyKey.FIELDS_TERMINATED.getValue())).findFirst();
        Optional<Property> second = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(p.getName(), HivePropertyKey.LINES_TERMINATED.getValue())).findFirst();
        if (first.isPresent() || second.isPresent()) {
            builder.append("ROW FORMAT DELIMITED");
            if (first.isPresent()) {
                String value = first.get().getValue();
                builder.append(StringUtils.LF).append("FIELDS TERMINATED BY ").append(String.format("'%s'", value));
            }
            if (second.isPresent()) {
                String value = second.get().getValue();
                builder.append(StringUtils.LF).append("LINES TERMINATED BY ").append(String.format("'%s'", value));
            }
        }

        // with serdeproperties
        String prefix = HivePropertyKey.SERDE_PROPS.getValue() + ".";
        List<Property> serdeProps = properties.stream()
                .filter(property -> property.getName().toLowerCase().startsWith(prefix))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(serdeProps)) {
            builder.append("\nWITH SERDEPROPERTIES (\n");
            String props = serdeProps.stream().map(property -> elementIndent
                    + StripUtils.addStrip(property.getName().substring(prefix.length()))
                    + " = "
                    + StripUtils.addStrip(property.getValue())).collect(joining(",\n"));
            builder.append(props).append("\n)\n");
        }

        return builder.toString();
    }

    public static String appendStoredFormat(CreateTable node) {
        StringBuilder stringBuilder = new StringBuilder();
        List<Property> properties = node.getProperties();
        if (node.isPropertyEmpty()) {
            return stringBuilder.toString();
        }
        Optional<Property> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(p.getName(), HivePropertyKey.STORAGE_FORMAT.getValue())).findFirst();
        Optional<Property> input = properties.stream().filter(
                p -> StringUtils.equalsIgnoreCase(p.getName(), HivePropertyKey.STORED_INPUT_FORMAT.getValue())).findFirst();
        Optional<Property> output = properties.stream().filter(
                p -> StringUtils.equalsIgnoreCase(p.getName(), HivePropertyKey.STORED_OUTPUT_FORMAT.getValue())).findFirst();
        if (first.isPresent()) {
            Property property = first.get();
            stringBuilder.append("STORED AS ").append(property.getValue());
        } else if (input.isPresent() || output.isPresent()) {
            stringBuilder.append("STORED AS ");
            if (input.isPresent()) {
                String value = input.get().getValue();
                stringBuilder.append(StringUtils.LF).append("INPUTFORMAT ").append(String.format("'%s'", value));
            }
            if (output.isPresent()) {
                String value = output.get().getValue();
                stringBuilder.append(StringUtils.LF).append("OUTPUTFORMAT ").append(String.format("'%s'", value));
            }
        }

        return stringBuilder.toString();
    }

    /**
     * append Location
     *
     * @param node
     * @return
     */
    public static String appendLocation(CreateTable node) {
        String property = PropertyKeyUtil.getProperty(node, HivePropertyKey.LOCATION);
        if (StringUtils.isBlank(property)) {
            return StringUtils.EMPTY;
        }
        return "LOCATION " + StripUtils.addStrip(property);
    }
}
