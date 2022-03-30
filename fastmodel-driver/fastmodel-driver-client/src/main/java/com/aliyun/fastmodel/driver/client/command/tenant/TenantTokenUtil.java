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

package com.aliyun.fastmodel.driver.client.command.tenant;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 用于租户调用的加签操作
 *
 * @author panguanjing
 * @date 2020/12/16
 */
@Slf4j
public class TenantTokenUtil {
    private static final String[] HEX_DIGITS = {
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
        "a", "b", "c", "d", "e", "f"
    };

    /**
     * 获取签名
     *
     * @param token    token
     * 信息
     * @param urlQuery url查询内容
     * @return signature
     */

    private static final String PLATFORM_FILE_ENCODING = "UTF-8";

    public static String getSignature(String token, String urlQuery) {
        String u = null;
        try {
            if (StringUtils.isNotBlank(urlQuery)) {
                u = URLDecoder.decode(urlQuery, PLATFORM_FILE_ENCODING);
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("generate the signature failure", e);
        }
        String[] paramArray = new String[] {};
        if (StringUtils.isNotBlank(u)) {
            String[] queryArray = u.split("&");
            paramArray = ArrayUtils.addAll(queryArray, paramArray);
        }
        //sort for every time is consistent
        Arrays.sort(paramArray);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(token);
        stringBuilder.append(":");

        for (String s : paramArray) {
            stringBuilder.append(s);
            stringBuilder.append("&");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        stringBuilder.append(":");
        stringBuilder.append(token);

        return signature(stringBuilder.toString());
    }

    public static String signature(String content) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            digest.update(content.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("generate signature failure", e);
        }
        return byteArrayToHexString(digest.digest());
    }

    private static String byteArrayToHexString(byte[] digest) {
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(byteToHexString(b));
        }
        return sb.toString();
    }

    private static String byteToHexString(byte b) {
        int n = b;
        if (n < 0) {
            n = 256 + n;
        }
        return HEX_DIGITS[n / 16] + HEX_DIGITS[n % 16];
    }
}
