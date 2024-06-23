package com.aliyun.fastmodel.transform.api.client.dto.index;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/2/19
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndexKey {
    private String column;
    private String expression;
    private Long length;
    private IndexSortType sortType;
}
