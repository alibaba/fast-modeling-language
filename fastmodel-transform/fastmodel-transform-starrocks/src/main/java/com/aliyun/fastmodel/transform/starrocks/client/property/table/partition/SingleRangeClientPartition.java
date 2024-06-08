package com.aliyun.fastmodel.transform.starrocks.client.property.table.partition;

import java.util.LinkedHashMap;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * range partition value
 *
 * @author panguanjing
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SingleRangeClientPartition {
    /**
     * column name
     */
    private String name;
    /**
     * if not exists
     */
    private boolean ifNotExists;
    /**
     * partition key
     */
    private BaseClientPartitionKey partitionKey;
    /**
     * properties
     */
    private LinkedHashMap<String, String> properties;



}
