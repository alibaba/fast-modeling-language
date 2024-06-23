package com.aliyun.fastmodel.transform.oceanbase.client.property.range;

import java.util.List;

import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.SingleRangeClientPartition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * RangePartitionElementClient
 *
 * @author panguanjing
 * @date 2024/2/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RangePartitionElementClient {

    private Long id;

    private SingleRangeClientPartition singleRangeClientPartition;

    private List<SubRangePartitionElementClient> rangeSubPartitionElementClients;
}
