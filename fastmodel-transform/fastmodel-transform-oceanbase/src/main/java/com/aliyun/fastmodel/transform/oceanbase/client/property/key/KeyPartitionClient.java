package com.aliyun.fastmodel.transform.oceanbase.client.property.key;

import java.util.List;

import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.hash.HashPartitionElementClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * KeyPartitionClient
 *
 * @author panguanjing
 * @date 2024/2/26
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeyPartitionClient {

    private Long partitionCount;

    private List<String> columnList;

    private SubPartitionClient subPartitionClient;

    private List<HashPartitionElementClient> partitionElementClientList;
}
