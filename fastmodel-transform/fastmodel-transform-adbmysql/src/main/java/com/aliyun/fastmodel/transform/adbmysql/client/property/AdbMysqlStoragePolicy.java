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

package com.aliyun.fastmodel.transform.adbmysql.client.property;

import com.aliyun.fastmodel.transform.adbmysql.format.AdbMysqlPropertyKey;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/3/25
 */
public class AdbMysqlStoragePolicy extends BaseClientProperty<String> {

    public AdbMysqlStoragePolicy() {
        this.setKey(AdbMysqlPropertyKey.STORAGE_POLICY.getValue());
    }

    @Override
    public String valueString() {
        return value;
    }

    @Override
    public void setValueString(String value) {
        this.setValue(value);
    }
}
