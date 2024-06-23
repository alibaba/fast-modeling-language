package com.aliyun.fastmodel.transform.api.extension.client.property.table.partition;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 子梁
 * @date 2023/12/26
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnExpressionClientPartition {

    /**
     * col list
     */
    private List<String> columnNameList;

}
