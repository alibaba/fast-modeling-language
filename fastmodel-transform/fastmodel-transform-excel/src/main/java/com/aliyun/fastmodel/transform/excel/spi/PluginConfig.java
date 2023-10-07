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

package com.aliyun.fastmodel.transform.excel.spi;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 定义了关于插件配置信息处理
 *
 * @author panguanjing
 * @date 2021/5/16
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PluginConfig {

    /**
     * 插件名字
     *
     * @return 定义插件的名字
     */
    String name() default "";

    /**
     * 导出名字
     *
     * @return
     */
    String exportName() default "undefined";

    /**
     * 模板的名字
     *
     * @return
     */
    String templateName() default "undefined";

    /**
     * sheet的配置
     *
     * @return {@link SheetConfig}
     */
    SheetConfig[] sheet();
}
