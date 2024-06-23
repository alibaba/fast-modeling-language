package com.aliyun.fastmodel.transform.oceanbase.client.property.range;

import java.util.List;

import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/2/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RangePartitionClient {

    private List<String> columns;

    private Long partitionCount;

    private String baseExpression;

    private List<RangePartitionElementClient> rangePartitionElementClients;

    private SubPartitionClient rangeSubPartitionClient;
}
