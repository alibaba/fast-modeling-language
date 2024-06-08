package com.aliyun.fastmodel.transform.starrocks.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;

/**
 * StarRocksContext
 *
 * @author panguanjing
 * @date 2023/9/5
 */
public class StarRocksContext extends TransformContext {
    public StarRocksContext(TransformContext context) {
        super(context);
    }

    public StarRocksContext(Builder tBuilder) {
        super(tBuilder);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        @Override
        public StarRocksContext build() {
            return new StarRocksContext(this);
        }
    }
}
