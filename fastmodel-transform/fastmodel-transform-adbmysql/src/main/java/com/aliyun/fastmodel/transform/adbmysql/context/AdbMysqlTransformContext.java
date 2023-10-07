package com.aliyun.fastmodel.transform.adbmysql.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;
import lombok.Getter;
import lombok.Setter;

/**
 * adbMysql Transform context
 *
 * @author panguanjing
 * @date 2023/2/11
 */
@Getter
@Setter
public class AdbMysqlTransformContext extends TransformContext {

    public AdbMysqlTransformContext(TransformContext context) {
        super(context);
    }

    public AdbMysqlTransformContext(Builder builder) {
        super(builder);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        @Override
        public AdbMysqlTransformContext build() {
            return new AdbMysqlTransformContext(this);
        }
    }

}
