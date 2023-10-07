package com.aliyun.fastmodel.transform.hologres.client.property;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/6/27
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnStatus {
    /**
     * column name
     */
    private String columnName;
    /**
     * status
     */
    private Status status;

    public static List<ColumnStatus> of(String value, Status defaultStatus) {
        if (StringUtils.isBlank(value)) {
            return Collections.emptyList();
        }
        List<String> list = Splitter.on(",").splitToList(value);
        List<ColumnStatus> columnStatuses = Lists.newArrayList();
        for (String s : list) {
            List<String> splitToList = Splitter.on(":").splitToList(s);
            ColumnStatus columnStatus = null;
            if (splitToList.size() > 1) {
                columnStatus = new ColumnStatus(
                    splitToList.get(0),
                    Status.getByValue(splitToList.get(1))
                );
            } else {
                columnStatus = new ColumnStatus(
                    splitToList.get(0),
                    defaultStatus
                );
            }
            columnStatuses.add(columnStatus);
        }
        return columnStatuses;
    }

}
