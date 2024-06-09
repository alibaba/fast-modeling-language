package com.aliyun.fastmodel.transform.starrocks.client.property.table.partition;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

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
