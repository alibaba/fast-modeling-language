package com.aliyun.fastmodel.transform.oceanbase.client.property.hash;

import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionElementClient;
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
public class SubHashPartitionElementClient extends SubPartitionElementClient {
    /**
     * name
     */
    private String qualifiedName;

    /**
     * engine
     */
    private StringProperty stringProperty;
}
