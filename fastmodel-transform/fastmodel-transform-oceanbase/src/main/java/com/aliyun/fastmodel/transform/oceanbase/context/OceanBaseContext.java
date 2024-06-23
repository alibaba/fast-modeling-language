package com.aliyun.fastmodel.transform.oceanbase.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;

/**
 * OceanBaseContext
 *
 * @author panguanjing
 * @date 2024/2/2
 */
public class OceanBaseContext extends TransformContext {
    public OceanBaseContext(TransformContext context) {
        super(context);
    }

    public OceanBaseContext(Builder tBuilder) {
        super(tBuilder);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        @Override
        public OceanBaseContext build() {
            return new OceanBaseContext(this);
        }
    }

}
