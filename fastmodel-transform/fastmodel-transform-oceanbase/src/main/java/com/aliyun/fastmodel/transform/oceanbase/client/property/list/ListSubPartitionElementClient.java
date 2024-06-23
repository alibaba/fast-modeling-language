package com.aliyun.fastmodel.transform.oceanbase.client.property.list;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ListSubPartitionElementClient
 *
 * @author panguanjing
 * @date 2024/2/26
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ListSubPartitionElementClient {
    private String qualifiedName;

    private Boolean defaultListExpr;

    private List<String> expressionList;

    private String engine;
}
