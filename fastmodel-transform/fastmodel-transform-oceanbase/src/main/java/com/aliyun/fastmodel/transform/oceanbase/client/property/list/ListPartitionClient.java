package com.aliyun.fastmodel.transform.oceanbase.client.property.list;

import java.util.List;

import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ListPartitionClient
 *
 * @author panguanjing
 * @date 2024/2/23
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ListPartitionClient {
    private String expression;

    private List<String> columns;

    private Long partitionCount;

    private SubPartitionClient subPartitionClient;

    private List<ListPartitionElementClient> partitionElementClientList;
}
