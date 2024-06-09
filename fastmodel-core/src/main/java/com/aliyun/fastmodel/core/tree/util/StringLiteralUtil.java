package com.aliyun.fastmodel.core.tree.util;

/**
 * 一些字符串字面量的工具类
 *
 * @author panguanjing
 * @date 2024/1/16
 */
public class StringLiteralUtil {
    public static final String SINGLE = "'";
    public static final String PREFIX = "`";
    public static final String DOUBLE_QUOTE = "\"";

    /**
     * 将字符串坐下strip
     *
     * @param src 原始字符串
     * @return 处理后的字符串
     */
    public static String strip(String src) {
        String prefix = SINGLE;
        if (src.startsWith(prefix)) {
            return stripOne(src, prefix);
        }
        prefix = DOUBLE_QUOTE;
        if (src.startsWith(prefix)) {
            return stripOne(src, prefix);
        }
        prefix = PREFIX;
        if (src.startsWith(prefix)) {
            return stripOne(src, prefix);
        }
        return src;
    }

    private static String unquote(String value) {
        return value.substring(1, value.length() - 1)
            .replace("''", "'");
    }

    private static String stripOne(String src, String prefix) {
        if (src.startsWith(prefix) && src.endsWith(prefix)) {
            return unquote(src);
        }
        return src;
    }
}
