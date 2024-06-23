package com.aliyun.fastmodel.transform.oceanbase.client.property.hash;

import java.util.List;

import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * HashPartitionClient
 *
 * @author panguanjing
 * @date 2024/2/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HashPartitionClient {

    private Long partitionCount;

    private String expression;

    private SubPartitionClient subPartitionClient;

    private List<HashPartitionElementClient> partitionElementClientList;
}
