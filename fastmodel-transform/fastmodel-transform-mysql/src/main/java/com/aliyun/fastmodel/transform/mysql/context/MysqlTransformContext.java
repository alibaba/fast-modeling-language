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

package com.aliyun.fastmodel.transform.mysql.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;
import lombok.Getter;

/**
 * mysql的转换上下文
 *
 * @author panguanjing
 * @date 2021/6/24
 */
@Getter
public class MysqlTransformContext extends TransformContext {

    public static final int DEFAULT_LENGTH = 128;
   
    private Integer varcharLength = DEFAULT_LENGTH;

    private boolean autoIncrement = false;

    private boolean generateForeignKey = false;

    protected MysqlTransformContext(Builder mysqlBuilder) {
        super(mysqlBuilder);
        varcharLength = mysqlBuilder.varcharLength;
        autoIncrement = mysqlBuilder.autoIncrement;
        generateForeignKey = mysqlBuilder.generateForeignKey;
    }

    public MysqlTransformContext(TransformContext context) {
        super(context);
        if (context instanceof MysqlTransformContext) {
            MysqlTransformContext mysqlTransformContext = (MysqlTransformContext)context;
            varcharLength = mysqlTransformContext.varcharLength;
            autoIncrement = mysqlTransformContext.autoIncrement;
            generateForeignKey = mysqlTransformContext.generateForeignKey;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        private Integer varcharLength = DEFAULT_LENGTH;

        private boolean autoIncrement;

        private boolean generateForeignKey;

        public Builder varcharLength(Integer varcharLength) {
            this.varcharLength = varcharLength;
            return this;
        }

        public Builder autoIncrement(boolean autoIncrement) {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public Builder generateForeignKey(boolean generateForeignKey) {
            this.generateForeignKey = generateForeignKey;
            return this;
        }

        @Override
        public MysqlTransformContext build() {
            return new MysqlTransformContext(this);
        }

    }

}
