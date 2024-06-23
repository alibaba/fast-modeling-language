package com.aliyun.fastmodel.transform.api.extension.client.property.table.partition;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * ArrayClientPartitionKey
 *
 * @author panguanjing
 * @date 2023/10/17
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ArrayClientPartitionKey extends BaseClientPartitionKey {

    private List<List<PartitionClientValue>> partitionValue;
}
