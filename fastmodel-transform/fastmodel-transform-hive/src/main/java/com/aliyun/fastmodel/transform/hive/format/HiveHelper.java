package com.aliyun.fastmodel.transform.hive.format;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.util.PropertyKeyUtil;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

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

    public static String appendRowFormat(CreateTable node) {
        StringBuilder builder = new StringBuilder();
        List<Property> properties = node.getProperties();
        if (node.isPropertyEmpty()) {
            return StringUtils.EMPTY;
        }

        Optional<Property> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(p.getName(), HivePropertyKey.FIELDS_TERMINATED.getValue())).findFirst();

        Optional<Property> second = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(p.getName(), HivePropertyKey.LINES_TERMINATED.getValue())).findFirst();
        if (first.isPresent() || second.isPresent()) {
            builder.append("ROW FORMAT DELIMITED");
        } else {
            return builder.toString();
        }
        if (first.isPresent()) {
            String value = first.get().getValue();
            builder.append(StringUtils.LF).append("FIELDS TERMINATED BY ").append(String.format("'%s'", value));
        }
        if (second.isPresent()) {
            String value = second.get().getValue();
            builder.append(StringUtils.LF).append("LINES TERMINATED BY ").append(String.format("'%s'", value));
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
        if (!first.isPresent()) {
            return stringBuilder.toString();
        }
        Property property = first.get();
        stringBuilder.append("STORED AS ").append(property.getValue());
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
