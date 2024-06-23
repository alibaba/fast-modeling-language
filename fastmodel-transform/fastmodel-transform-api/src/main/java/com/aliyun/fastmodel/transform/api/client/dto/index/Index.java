package com.aliyun.fastmodel.transform.api.client.dto.index;

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Index 索引
 *
 * @author panguanjing
 * @date 2024/2/19
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Index {
    /**
     * 索引名称
     */
    private String name;

    /**
     * 列
     */
    private List<IndexKey> indexKeys;


    /**
     * client properties
     */
    private List<BaseClientProperty> properties;

}
