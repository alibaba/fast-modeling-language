package com.aliyun.fastmodel.transform.oceanbase.client.property.range;

import java.util.List;

import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * RangeSubPartitionClient
 *
 * @author panguanjing
 * @date 2024/2/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubRangePartitionClient extends SubPartitionClient {
    private String expression;

    private List<String> columnList;

    private List<RangePartitionElementClient> singleRangePartitionList;
}