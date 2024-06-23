package com.aliyun.fastmodel.transform.doris.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;

/**
 * doris context
 *
 * @author panguanjing
 * @date 2024/1/20
 */
public class DorisContext extends TransformContext {
    public DorisContext(TransformContext context) {
        super(context);
    }

    public DorisContext(Builder tBuilder) {
        super(tBuilder);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        @Override
        public DorisContext build() {
            return new DorisContext(this);
        }
    }
}
