package com.aliyun.fastmodel.transform.sqlite.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;

/**
 * SqliteContext
 *
 * @author panguanjing
 * @date 2023/8/14
 */
public class SqliteContext extends TransformContext {
    public SqliteContext(TransformContext context) {
        super(context);
    }

    public SqliteContext(Builder builder) {
        super(builder);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        @Override
        public SqliteContext build() {
            return new SqliteContext(this);
        }
    }

}
