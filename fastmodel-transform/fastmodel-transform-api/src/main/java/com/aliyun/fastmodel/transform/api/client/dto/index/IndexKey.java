package com.aliyun.fastmodel.transform.api.client.dto.index;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 索引key定义
 *
 * @author panguanjing
 * @date 2024/2/19
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndexKey {
    /**
     * 列
     */
    private String column;
    /**
     * 表达式
     */
    private String expression;
    /**
     * 长度
     */
    private Long length;
    /**
     * 排序类型
     */
    private IndexSortType sortType;
}
