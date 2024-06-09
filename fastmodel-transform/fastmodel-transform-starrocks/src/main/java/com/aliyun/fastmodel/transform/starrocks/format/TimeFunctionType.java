package com.aliyun.fastmodel.transform.starrocks.format;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2023/12/26
 */
@RequiredArgsConstructor
public enum TimeFunctionType {

    DATE_TRUNC("date_trunc"),

    TIME_SLICE("time_slice");

    @Getter
    private final String value;

    public static TimeFunctionType getByValue(String value) {
        TimeFunctionType[] timeFunctionTypes = TimeFunctionType.values();
        for (TimeFunctionType TimeFunctionType : timeFunctionTypes) {
            if (StringUtils.equalsIgnoreCase(TimeFunctionType.getValue(), value)) {
                return TimeFunctionType;
            }
        }
        return null;
    }

}
