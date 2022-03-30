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

package com.aliyun.fastmodel.transform.api.dialect;

import com.google.common.base.Strings;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.BooleanUtils;

/**
 * 方言名称
 *
 * @author panguanjing
 * @date 2020/10/16
 */
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode
public class DialectNode extends GenericDialectNode<String> {
    /**
     * 是否可执行
     */
    private Boolean executable;

    public DialectNode(String node, Boolean executable) {
        super(node);
        this.executable = executable;
    }

    public DialectNode(String node) {
        this(node, true);
    }

    /**
     * Node是否可以执行的处理
     *
     * @return true代表可执行
     */
    public boolean isExecutable() {
        return !Strings.isNullOrEmpty(getNode()) &&
            BooleanUtils.toBooleanDefaultIfNull(executable, true);
    }

}
