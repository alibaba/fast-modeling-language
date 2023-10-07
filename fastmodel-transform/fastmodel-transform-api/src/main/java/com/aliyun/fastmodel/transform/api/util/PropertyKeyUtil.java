package com.aliyun.fastmodel.transform.api.util;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/18
 */
public class PropertyKeyUtil {

    /**
     * 从node中获取指定key的信息
     * @param node
     * @param propertyKey
     * @return
     */
    public static String getProperty(CreateTable node, PropertyKey propertyKey) {
        if (node.isPropertyEmpty() || propertyKey == null) {
            return StringUtils.EMPTY;
        }
        List<Property> properties = node.getProperties();
        Optional<Property> first = properties.stream().filter(p -> {
            return StringUtils.equalsIgnoreCase(p.getName(), propertyKey.getValue());
        }).findFirst();
        if (!first.isPresent()) {
            return StringUtils.EMPTY;
        }
        return first.get().getValue();
    }

}
