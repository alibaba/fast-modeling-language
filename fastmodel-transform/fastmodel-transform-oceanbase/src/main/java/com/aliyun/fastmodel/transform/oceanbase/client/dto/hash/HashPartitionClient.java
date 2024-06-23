package com.aliyun.fastmodel.transform.oceanbase.client.dto.hash;

import java.util.List;

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

    private SubHashPartitionClient subHashPartitionClient;

    private List<HashPartitionElementClient> partitionElementClientList;
}
