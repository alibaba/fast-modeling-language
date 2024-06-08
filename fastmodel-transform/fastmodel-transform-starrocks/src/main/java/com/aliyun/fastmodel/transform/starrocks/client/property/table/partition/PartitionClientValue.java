package com.aliyun.fastmodel.transform.starrocks.client.property.table.partition;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/10/23
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PartitionClientValue {
    /**
     * max value
     */
    private boolean maxValue;
    /**
     * value
     */
    private String value;
}
