package com.aliyun.fastmodel.transform.starrocks.client.property.table.partition;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/10/16
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LessThanClientPartitionKey extends BaseClientPartitionKey{

    private boolean maxValue;

    private List<PartitionClientValue> partitionValueList;
}
