package com.aliyun.fastmodel.transform.starrocks.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.converter.BasePropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.DistributeBucketsNum;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.DistributeHash;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.ReplicationNum;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import com.google.common.collect.Maps;

/**
 * StarRocksPropertyConverter
 *
 * @author panguanjing
 * @date 2023/9/16
 */
public class StarRocksPropertyConverter extends BasePropertyConverter {

    private Map<String, Function<String, BaseClientProperty>> functionMap = Maps.newHashMap();

    public StarRocksPropertyConverter() {
        init();
    }

    private void init() {
        functionMap.put(StarRocksProperty.TABLE_DISTRIBUTED_BUCKETS.getValue(),
            (k) -> {
                DistributeBucketsNum distributeBucketsNum = new DistributeBucketsNum();
                distributeBucketsNum.setValueString(k);
                return distributeBucketsNum;
            });

        functionMap.put(StarRocksProperty.TABLE_DISTRIBUTED_HASH.getValue(), (k) -> {
            DistributeHash distributeHash = new DistributeHash();
            distributeHash.setValueString(k);
            return distributeHash;
        });

        functionMap.put(StarRocksProperty.TABLE_REPLICATION_NUM.getValue(), (k) -> {
            ReplicationNum distributeHash = new ReplicationNum();
            distributeHash.setValueString(k);
            return distributeHash;
        });
    }

    @Override
    protected Map<String, Function<String, BaseClientProperty>> getFunctionMap() {
        return functionMap;
    }
}
