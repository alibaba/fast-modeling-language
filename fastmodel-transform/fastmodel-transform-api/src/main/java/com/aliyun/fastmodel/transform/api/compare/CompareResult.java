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

package com.aliyun.fastmodel.transform.api.compare;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.collections.CollectionUtils;

/**
 * 比对的结果信息内容
 *
 * @author panguanjing
 * @date 2021/9/17
 */
@Getter
@Setter
@ToString
public class CompareResult {

    /**
     * before statement
     */
    private final BaseStatement beforeStatement;

    /**
     * after statement
     */
    private final BaseStatement afterStatement;

    /**
     * diff statements
     */
    private final List<BaseStatement> diffStatements;

    public CompareResult(BaseStatement beforeStatement, BaseStatement originAfter,
                         List<BaseStatement> diff) {
        this.beforeStatement = beforeStatement;
        afterStatement = originAfter;
        diffStatements = diff;
    }

    public boolean diffEmpty() {
        return CollectionUtils.isEmpty(diffStatements);
    }

}
