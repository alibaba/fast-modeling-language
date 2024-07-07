package com.aliyun.fastmodel.transform.hologres.dialect;

import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * hologres version
 *
 * @author panguanjing
 * @date 2023/6/24
 */
public enum HologresVersion implements IVersion {
    /**
     * 1.0
     */
    V1(Constants.V1),
    /**
     * 2.0
     */
    V2(Constants.V2);

    @Getter
    private final String value;

    HologresVersion(String value) {this.value = value;}

    public static class Constants {
        public static final String V1 = "1.0";
        public static final String V2 = "2.0";
    }

    public static HologresVersion getByValue(String value) {
        HologresVersion[] hologresVersions = HologresVersion.values();
        for (HologresVersion version : hologresVersions) {
            if (StringUtils.equalsIgnoreCase(version.getValue(), value)) {
                return version;
            }
        }
        return HologresVersion.V1;
    }

    @Override
    public String getName() {
        return this.getValue();
    }

}
