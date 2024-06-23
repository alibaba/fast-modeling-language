package com.aliyun.fastmodel.transform.oceanbase.client.property.list;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ListPartitionElementClient
 *
 * @author panguanjing
 * @date 2024/2/26
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ListPartitionElementClient {
    private String qualifiedName;

    private Boolean defaultExpr;

    private List<String> expressionList;

    private Long num;

    private String engine;

    private List<SubListPartitionElementClient> subPartitionElementList;
}
