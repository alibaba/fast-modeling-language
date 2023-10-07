package com.aliyun.fastmodel.transform.spark.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;
import lombok.Getter;

/**
 * https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-hiveformat.html
 * https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html
 *
 * @author panguanjing
 * @date 2023/2/13
 */
@Getter
public class SparkTransformContext extends TransformContext {

    private SparkTableFormat sparkTableFormat = SparkTableFormat.HIVE_FORMAT;

    public SparkTransformContext(TransformContext context) {
        super(context);
        if (context instanceof SparkTransformContext) {
            SparkTransformContext sparkTransformContext = (SparkTransformContext)context;
            this.sparkTableFormat = sparkTransformContext.getSparkTableFormat();
        }
    }

    protected SparkTransformContext(Builder tBuilder) {
        super(tBuilder);
        this.sparkTableFormat = tBuilder.sparkTableFormat;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        private SparkTableFormat sparkTableFormat = SparkTableFormat.HIVE_FORMAT;

        public Builder tableFormat(SparkTableFormat sparkTableFormat) {
            this.sparkTableFormat = sparkTableFormat;
            return this;
        }

        @Override
        public SparkTransformContext build() {
            return new SparkTransformContext(this);
        }
    }
}
