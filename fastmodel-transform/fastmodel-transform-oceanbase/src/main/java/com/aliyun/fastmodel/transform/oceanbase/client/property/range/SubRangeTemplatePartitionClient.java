package com.aliyun.fastmodel.transform.oceanbase.client.property.range;

import java.util.List;

import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionElementClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * SubRangeTemplatePartition
 *
 * @author panguanjing
 * @date 2024/2/18
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubRangeTemplatePartitionClient extends SubPartitionClient {

    private String expression;

    private List<String> columnList;

    private List<SubPartitionElementClient> subPartitionElementClientList;

}
