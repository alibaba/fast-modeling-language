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

package com.aliyun.fastmodel.transform.graph.context;

import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.graph.exporter.ExportName;
import lombok.Getter;

/**
 * 图的转换的操作内容
 *
 * @author panguanjing
 * @date 2021/12/15
 */
public class GraphTransformContext extends TransformContext {

    @Getter
    private ExportName exportName = ExportName.JSON;

    public GraphTransformContext(TransformContext context) {
        super(context);
        if (context instanceof GraphTransformContext) {
            GraphTransformContext c = (GraphTransformContext)context;
            exportName = c.exportName;
        }
    }

    protected GraphTransformContext(Builder builder) {
        super(builder);
        exportName = builder.exportName;
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TransformContext.Builder<Builder> {

        private ExportName exportName = ExportName.JSON;

        public Builder exportName(ExportName exportName) {
            this.exportName = exportName;
            return this;
        }

        @Override
        public GraphTransformContext build() {
            return new GraphTransformContext(this);
        }

    }

}
