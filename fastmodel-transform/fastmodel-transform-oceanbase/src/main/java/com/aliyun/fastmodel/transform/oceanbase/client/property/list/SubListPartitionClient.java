package com.aliyun.fastmodel.transform.oceanbase.client.property.list;

import java.util.List;

import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * SubListPartitionClient
 *
 * @author panguanjing
 * @date 2024/2/26
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubListPartitionClient extends SubPartitionClient {

    private String expression;

    private List<String> columnList;
}
