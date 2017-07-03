package com.deppon.hadoop.sqoopx.core.mapreduce;

import java.util.regex.Pattern;

/**
 * Created by meepai on 2017/6/27.
 */
public class FieldFormatter {

    private static final Pattern REPLACE_PATTERN = Pattern.compile("\\n|\\r|\01");

    /**
     * 移除hive 字段分割，换行等。
     * @param str
     * @param delimiters
     * @return
     */
    public static String hiveStringDropDelims(String str, DelimiterSet delimiters) {
        return hiveStringReplaceDelims(str, "", delimiters);
    }

    /**
     * 使用指定的替换字符 来进行替换
     * @param str
     * @param replacement
     * @param delimiters
     * @return
     */
    public static String hiveStringReplaceDelims(String str, String replacement, DelimiterSet delimiters) {
        String droppedDelims = REPLACE_PATTERN.matcher(str).replaceAll(replacement);
        return escapeAndEnclose(droppedDelims, delimiters);
    }

    /**
     * 为字符串增加转义和引号
     * @param str
     * @param delimiters
     * @return
     */
    public static String escapeAndEnclose(String str, DelimiterSet delimiters) {
        char escape = delimiters.getEscapedBy();
        char enclose = delimiters.getEnclosedBy();
        boolean encloseRequired = delimiters.isEncloseRequired();

        // true if we can use an escape character.
        boolean escapingLegal = DelimiterSet.NULL_CHAR != escape;
        String withEscapes;
        if (null == str) {
            return null;
        }
        if (escapingLegal) {
            // escaping is legal. Escape any instances of the escape char itself.
            withEscapes = str.replace("" + escape, "" + escape + escape);
        } else {
            // no need to double-escape
            withEscapes = str;
        }

        if (DelimiterSet.NULL_CHAR == enclose) {
            // The enclose-with character was left unset, so we can't enclose items.
            if (escapingLegal) {
                // If the user has used the fields-terminated-by or
                // lines-terminated-by characters in the string, escape them if we
                // have an escape character.
                String fields = "" + delimiters.getFieldDelim();
                String lines = "" + delimiters.getRecordDelim();
                withEscapes = withEscapes.replace(fields, "" + escape + fields);
                withEscapes = withEscapes.replace(lines, "" + escape + lines);
            }
            // No enclosing possible, so now return this.
            return withEscapes;
        }
        // if we have an enclosing character, and escaping is legal, then the
        // encloser must always be escaped.
        if (escapingLegal) {
            withEscapes = withEscapes.replace("" + enclose, "" + escape + enclose);
        }

        boolean actuallyDoEnclose = encloseRequired;
        if (!actuallyDoEnclose) {
            // check if the string requires enclosing.
            char [] mustEncloseFor = new char[2];
            mustEncloseFor[0] = delimiters.getFieldDelim();
            mustEncloseFor[1] = delimiters.getRecordDelim();
            for (char reason : mustEncloseFor) {
                if (str.indexOf(reason) != -1) {
                    actuallyDoEnclose = true;
                    break;
                }
            }
        }
        if (actuallyDoEnclose) {
            return "" + enclose + withEscapes + enclose;
        } else {
            return withEscapes;
        }
    }
}
