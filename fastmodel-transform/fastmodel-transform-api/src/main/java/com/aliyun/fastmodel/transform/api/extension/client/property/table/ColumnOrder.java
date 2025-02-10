package com.aliyun.fastmodel.transform.api.extension.client.property.table;

import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.statement.select.order.Ordering;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * ColumnOrder
 *
 * @author panguanjing
 * @date 2024/4/19
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnOrder {
    /**
     * 列名
     */
    private String columnName;

    /**
     * 排序方式
     */
    private Ordering order;

    /**
     * column:order, column
     *
     * @param value
     * @return {@see ColumnOrder}
     */
    public static List<ColumnOrder> of(String value) {
        if (StringUtils.isBlank(value)) {
            return Collections.emptyList();
        }
        String val = StripUtils.removeDoubleStrip(value);
        List<String> list = Splitter.on(",").splitToList(val);
        List<ColumnOrder> columnOrders = Lists.newArrayList();
        for (String s : list) {
            List<String> splitToList = Splitter.on(":").splitToList(s);
            ColumnOrder columnStatus = null;
            if (splitToList.size() > 1) {
                columnStatus = new ColumnOrder(
                    splitToList.get(0).trim(),
                    Ordering.getByCode(splitToList.get(1).trim())
                );
            } else {
                columnStatus = new ColumnOrder(
                    splitToList.get(0).trim(),
                    null
                );
            }
            columnOrders.add(columnStatus);
        }
        return columnOrders;
    }
}
