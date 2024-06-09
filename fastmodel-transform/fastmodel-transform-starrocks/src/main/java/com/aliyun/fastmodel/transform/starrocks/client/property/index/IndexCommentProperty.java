package com.aliyun.fastmodel.transform.starrocks.client.property.index;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;

/**
 * 索引描述信息属性
 *
 * @author panguanjing
 * @date 2023/12/18
 */
public class IndexCommentProperty extends BaseClientProperty<String> {

    public IndexCommentProperty() {
        setKey(StarRocksProperty.TABLE_INDEX_COMMENT.getValue());
    }

    @Override
    public String valueString() {
        return value;
    }

    @Override
    public void setValueString(String value) {
        this.setValue(value);
    }
}
