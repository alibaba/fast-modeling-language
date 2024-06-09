package com.aliyun.fastmodel.transform.starrocks.client.property.table.partition;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * list partition value
 *
 * @author 子梁
 * @date 2023/12/25
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ListClientPartition {

    /**
     * partition name
     */
    private String name;

    /**
     * partition key
     */
    private BaseClientPartitionKey partitionKey;

}
