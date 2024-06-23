package com.aliyun.fastmodel.transform.oceanbase.client.dto.hash;

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
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
public class HashPartitionElementClient {
    private String qualifiedName;

    private Long num;

    private StringProperty property;

    private List<SubHashPartitionElementClient> subPartitionList;
}
