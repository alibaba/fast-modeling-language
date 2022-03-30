/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.hologres.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;
import lombok.Getter;

/**
 * builder
 *
 * @author panguanjing
 * @date 2021/3/7
 */
public class HologresTransformContext extends TransformContext {

    public static final String COLUMN = "column";
    public static final long DEFAULT_SECONDS = 3153600000L;

    @Getter
    private String orientation = COLUMN;

    @Getter
    private Long timeToLiveInSeconds = DEFAULT_SECONDS;

    public HologresTransformContext(TransformContext context) {
        super(context);
        if (context instanceof HologresTransformContext) {
            HologresTransformContext hologresTransformContext = (HologresTransformContext)context;
            orientation = hologresTransformContext.getOrientation();
            timeToLiveInSeconds = hologresTransformContext.getTimeToLiveInSeconds();
        }
    }

    public HologresTransformContext(Builder builder) {
        super(builder);
        orientation = builder.getOrientation();
        timeToLiveInSeconds = builder.getTimeToLiveInSeconds();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        @Getter
        private String orientation = COLUMN;

        @Getter
        private Long timeToLiveInSeconds = DEFAULT_SECONDS;

        @Override
        public HologresTransformContext build() {
            return new HologresTransformContext(this);
        }

        public Builder orientation(String orientation) {
            this.orientation = orientation;
            return this;
        }

        public Builder timeToLiveInSeconds(Long timeToLiveInSeconds) {
            this.timeToLiveInSeconds = timeToLiveInSeconds;
            return this;
        }
    }
}
