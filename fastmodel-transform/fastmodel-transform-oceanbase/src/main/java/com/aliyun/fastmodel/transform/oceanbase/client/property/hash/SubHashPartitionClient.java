package com.aliyun.fastmodel.transform.oceanbase.client.property.hash;

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
public class SubHashPartitionClient extends SubPartitionClient {
    private String expression;
    private Long subpartitionCount;
}
