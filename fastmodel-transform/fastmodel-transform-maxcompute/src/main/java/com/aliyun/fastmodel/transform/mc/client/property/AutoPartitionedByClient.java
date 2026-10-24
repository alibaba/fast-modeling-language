package com.aliyun.fastmodel.transform.mc.client.property;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * AutoPartitionedByClient
 * @author panguanjing
 * @date 2025/12/7
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AutoPartitionedByClient {
    private String expression;
    private String alias;
}
