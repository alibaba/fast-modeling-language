package com.aliyun.fastmodel.transform.api.extension.client.property.table.partition;

import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * multi range partition value
 *
 * @author panguanjing
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MultiRangeClientPartition {
    /**
     * start
     */
    private String start;
    /**
     * end
     */
    private String end;
    /**
     * interval
     */
    private Long interval;
    /**
     * time enum
     */
    private DateTimeEnum dateTimeEnum;
}
