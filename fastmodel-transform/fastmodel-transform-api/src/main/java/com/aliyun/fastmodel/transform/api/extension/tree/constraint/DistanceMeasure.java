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

package com.aliyun.fastmodel.transform.api.extension.tree.constraint;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * DistanceMeasure
 *
 * @author panguanjing
 * @date 2024/10/3
 */
@Getter
public enum DistanceMeasure {
    SquaredL2("SquaredL2");

    private final String value;

    DistanceMeasure(String value) {this.value = value;}

    public static DistanceMeasure fromValue(String value) {
        DistanceMeasure[] values = DistanceMeasure.values();
        for (DistanceMeasure distanceMeasure : values) {
            if (StringUtils.equalsIgnoreCase(distanceMeasure.value, value)) {
                return distanceMeasure;
            }
        }
        return null;
    }
}
