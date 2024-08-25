/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.api.builder.merge.exception;

import lombok.Getter;

/**
 * merge error code
 *
 * @author panguanjing
 * @date 2024/5/1
 */
public enum MergeErrorCode {
    /**
     * create table not exists
     */
    ERR_CREATE_TABLE_NOT_EXISTS("Error: 'CREATE TABLE' statement not exists."),
    /**
     * 只允许有一个create table语句
     */
    ERR_TOO_MANY_CREATE_TABLES("Error: Only one 'CREATE TABLE' statement is allowed.");
    @Getter
    private final String message;

    MergeErrorCode(String message) {
        this.message = message;
    }

}
