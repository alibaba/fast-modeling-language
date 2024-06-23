package com.aliyun.fastmodel.transform.oceanbase.client.property.key;

import java.util.List;

import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionElementClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * SubKeyTemplatePartitionClient
 *
 * @author panguanjing
 * @date 2024/2/28
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubKeyTemplatePartitionClient extends SubPartitionClient {

    private List<String> columnList;

    private List<SubPartitionElementClient> subPartitionElementClients;
}
