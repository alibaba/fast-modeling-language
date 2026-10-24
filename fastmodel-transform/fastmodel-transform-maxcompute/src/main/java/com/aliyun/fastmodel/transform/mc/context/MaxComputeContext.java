/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;
import lombok.Getter;
import lombok.Setter;

/**
 * MaxComputeContext
 *
 * @author panguanjing
 * @date 2021/3/4
 */
@Getter
@Setter
public class MaxComputeContext extends TransformContext {

    private boolean useV1DataType;

    private String mcProject;

    private String mcSchema;

    private boolean keepConstraint;

    public static Builder builder() {
        return new Builder();
    }

    public MaxComputeContext(TransformContext parent) {
        super(parent);
        this.mcProject = parent.getDatabase();
        this.mcSchema = parent.getSchema();
        //没有使用bean copy的方式，是不想再引入其他类库，或者自己实现一套
        if (parent instanceof MaxComputeContext) {
            MaxComputeContext maxComputeContext = (MaxComputeContext)parent;
            useV1DataType = maxComputeContext.isUseV1DataType();
            mcProject = maxComputeContext.getMcProject();
            mcSchema = maxComputeContext.getMcSchema();
            keepConstraint = maxComputeContext.isKeepConstraint();
        }
    }

    protected MaxComputeContext(Builder builder) {
        super(builder);
        useV1DataType = builder.useV1DataType;
        this.mcSchema = builder.mcSchema;
        this.mcProject = builder.mcProject;
        this.keepConstraint = builder.keepConstraint;
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        /**
         * 是否使用1.0的数据类型
         */
        private boolean useV1DataType;

        /**
         * mc项目
         */
        private String mcProject;

        /**
         * mc的schema
         */
        private String mcSchema;

        /**
         * 是否保留constraint
         */
        private boolean keepConstraint;

        public Builder useV1DataType(boolean useV1DataType) {
            this.useV1DataType = useV1DataType;
            return this;
        }

        public Builder mcProject(String mcProject) {
            this.mcProject = mcProject;
            return this;
        }

        public Builder mcSchema(String mcSchema) {
            this.mcSchema = mcSchema;
            return this;
        }

        public Builder keepConstraint(boolean keepConstraint) {
            this.keepConstraint = keepConstraint;
            return this;
        }

        @Override
        public MaxComputeContext build() {
            return new MaxComputeContext(this);
        }

    }
}
