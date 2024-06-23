package com.aliyun.fastmodel.transform.oceanbase.client.property.hash;

import java.util.List;

import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionElementClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/2/28
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SubHashTemplatePartitionClient extends SubPartitionClient {

    private String expression;

    private List<SubPartitionElementClient> subPartitionElementClientList;
}
