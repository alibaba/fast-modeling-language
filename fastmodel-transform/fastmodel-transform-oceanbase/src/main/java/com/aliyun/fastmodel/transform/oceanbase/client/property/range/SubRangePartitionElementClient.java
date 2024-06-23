package com.aliyun.fastmodel.transform.oceanbase.client.property.range;

import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.SingleRangeClientPartition;
import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionElementClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * range sub partition element client
 *
 * @author panguanjing
 * @date 2024/2/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubRangePartitionElementClient extends SubPartitionElementClient {
    private SingleRangeClientPartition singleRangeClientPartition;
}
