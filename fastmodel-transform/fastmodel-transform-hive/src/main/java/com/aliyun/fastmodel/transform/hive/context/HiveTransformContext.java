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

package com.aliyun.fastmodel.transform.hive.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/1
 */
@Getter
@Setter
@ToString
public class HiveTransformContext extends TransformContext {

    private boolean enableConstraint;

    private boolean printProperty;

    public HiveTransformContext(TransformContext parent) {
        super(parent);
        this.printProperty = true;
        if (parent instanceof HiveTransformContext) {
            HiveTransformContext transformContext = (HiveTransformContext)parent;
            enableConstraint = transformContext.isEnableConstraint();
        }
    }

    protected HiveTransformContext(Builder tBuilder) {
        super(tBuilder);
        enableConstraint = tBuilder.enableConstraint;
        this.printProperty = tBuilder.printProperty;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {
        private boolean enableConstraint;

        //默认是打印
        private boolean printProperty = true;

        public Builder enableConstraint(boolean enableConstraint) {
            this.enableConstraint = enableConstraint;
            return this;
        }

        public Builder printProperty(boolean printProperty) {
            this.printProperty = printProperty;
            return this;
        }

        @Override
        public HiveTransformContext build() {
            return new HiveTransformContext(this);
        }
    }
}
