/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.postgresql.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;
import lombok.Getter;
import lombok.Setter;

/**
 * builder
 *
 * @author panguanjing
 */
@Getter
@Setter
public class PostgreSQLTransformContext extends TransformContext {

    public static final String COLUMN = "column";

    public static final long DEFAULT_SECONDS = 3153600000L;

    private String orientation = COLUMN;

    private Long timeToLiveInSeconds = DEFAULT_SECONDS;

    private boolean caseSensitive;

    public PostgreSQLTransformContext(TransformContext context) {
        super(context);
        if (context instanceof PostgreSQLTransformContext) {
            PostgreSQLTransformContext postgreSQLTransformContext = (PostgreSQLTransformContext)context;
            orientation = postgreSQLTransformContext.getOrientation();
            timeToLiveInSeconds = postgreSQLTransformContext.getTimeToLiveInSeconds();
            caseSensitive = postgreSQLTransformContext.isCaseSensitive();
        }
    }

    public PostgreSQLTransformContext(Builder builder) {
        super(builder);
        orientation = builder.getOrientation();
        timeToLiveInSeconds = builder.getTimeToLiveInSeconds();
        caseSensitive = builder.isCaseSensitive();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Getter
    public static class Builder extends TransformContext.Builder<Builder> {

        private String orientation = COLUMN;

        private Long timeToLiveInSeconds = DEFAULT_SECONDS;

        private boolean caseSensitive;

        private boolean useAlterTableSetSentence;

        @Override
        public PostgreSQLTransformContext build() {
            return new PostgreSQLTransformContext(this);
        }

        public Builder orientation(String orientation) {
            this.orientation = orientation;
            return this;
        }

        public Builder timeToLiveInSeconds(Long timeToLiveInSeconds) {
            this.timeToLiveInSeconds = timeToLiveInSeconds;
            return this;
        }

        public Builder caseSensitive(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
            return this;
        }

    }
}
