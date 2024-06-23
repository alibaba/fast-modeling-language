package com.aliyun.fastmodel.transform.oceanbase.client.converter.partition;

import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.client.property.SubPartitionElementClient;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.BaseSubPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubPartitionElement;

/**
 * converter
 * provider client to fml, and fml to client converter
 *
 * @author panguanjing
 * @date 2024/2/28
 */
public interface OceanBasePartitionClientConverter {
    /**
     * to subPartition client
     *
     * @param baseSubPartition
     * @return
     */
    SubPartitionClient toSubPartitionClient(BaseSubPartition baseSubPartition);

    /**
     * to baseSubPartition
     *
     * @param subPartitionClient
     * @return
     */
    BaseSubPartition getSubPartition(SubPartitionClient subPartitionClient);

    /**
     * toSubPartitionElementClient
     *
     * @param subPartitionElement
     * @return
     */
    SubPartitionElementClient toSubPartitionElementClient(SubPartitionElement subPartitionElement);

    /**
     * getSubPartitionElement
     *
     * @param subPartitionElementClient
     * @return
     */
    SubPartitionElement getSubPartitionElement(SubPartitionElementClient subPartitionElementClient);

}
