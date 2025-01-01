package com.aliyun.fastmodel.transform.flink.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;

/**
 * @author 子梁
 * @date 2024/5/15
 */
public class FlinkTransformContext extends TransformContext {

    public FlinkTransformContext(TransformContext context) {
        super(context);
    }

    public FlinkTransformContext(Builder tBuilder) {
        super(tBuilder);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        @Override
        public FlinkTransformContext build() {
            return new FlinkTransformContext(this);
        }
    }

}
