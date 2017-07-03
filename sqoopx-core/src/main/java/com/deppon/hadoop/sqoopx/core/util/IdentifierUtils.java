package com.deppon.hadoop.sqoopx.core.util;

import java.util.HashSet;

/**
 * Created by meepai on 2017/6/25.
 */
public class IdentifierUtils {

    /**
     * java保留字段
     */
    public static final HashSet<String> JAVA_RESERVED_WORDS;

    static {
        JAVA_RESERVED_WORDS = new HashSet<String>();
        JAVA_RESERVED_WORDS.add("abstract");
        JAVA_RESERVED_WORDS.add("assert");
        JAVA_RESERVED_WORDS.add("boolean");
        JAVA_RESERVED_WORDS.add("break");
        JAVA_RESERVED_WORDS.add("byte");
        JAVA_RESERVED_WORDS.add("case");
        JAVA_RESERVED_WORDS.add("catch");
        JAVA_RESERVED_WORDS.add("char");
        JAVA_RESERVED_WORDS.add("class");
        JAVA_RESERVED_WORDS.add("const");
        JAVA_RESERVED_WORDS.add("continue");
        JAVA_RESERVED_WORDS.add("default");
        JAVA_RESERVED_WORDS.add("do");
        JAVA_RESERVED_WORDS.add("double");
        JAVA_RESERVED_WORDS.add("else");
        JAVA_RESERVED_WORDS.add("enum");
        JAVA_RESERVED_WORDS.add("extends");
        JAVA_RESERVED_WORDS.add("false");
        JAVA_RESERVED_WORDS.add("final");
        JAVA_RESERVED_WORDS.add("finally");
        JAVA_RESERVED_WORDS.add("float");
        JAVA_RESERVED_WORDS.add("for");
        JAVA_RESERVED_WORDS.add("goto");
        JAVA_RESERVED_WORDS.add("if");
        JAVA_RESERVED_WORDS.add("implements");
        JAVA_RESERVED_WORDS.add("import");
        JAVA_RESERVED_WORDS.add("instanceof");
        JAVA_RESERVED_WORDS.add("int");
        JAVA_RESERVED_WORDS.add("interface");
        JAVA_RESERVED_WORDS.add("long");
        JAVA_RESERVED_WORDS.add("native");
        JAVA_RESERVED_WORDS.add("new");
        JAVA_RESERVED_WORDS.add("null");
        JAVA_RESERVED_WORDS.add("package");
        JAVA_RESERVED_WORDS.add("private");
        JAVA_RESERVED_WORDS.add("protected");
        JAVA_RESERVED_WORDS.add("public");
        JAVA_RESERVED_WORDS.add("return");
        JAVA_RESERVED_WORDS.add("short");
        JAVA_RESERVED_WORDS.add("static");
        JAVA_RESERVED_WORDS.add("strictfp");
        JAVA_RESERVED_WORDS.add("super");
        JAVA_RESERVED_WORDS.add("switch");
        JAVA_RESERVED_WORDS.add("synchronized");
        JAVA_RESERVED_WORDS.add("this");
        JAVA_RESERVED_WORDS.add("throw");
        JAVA_RESERVED_WORDS.add("throws");
        JAVA_RESERVED_WORDS.add("transient");
        JAVA_RESERVED_WORDS.add("true");
        JAVA_RESERVED_WORDS.add("try");
        JAVA_RESERVED_WORDS.add("void");
        JAVA_RESERVED_WORDS.add("volatile");
        JAVA_RESERVED_WORDS.add("while");
        // not strictly reserved words, but collides with
        // our imports
//        JAVA_RESERVED_WORDS.add("Text");
        //Fix For Issue SQOOP-2839
//        JAVA_RESERVED_WORDS.add("PROTOCOL_VERSION");
    }

    /**
     * 转换成java标识符
     * @param name
     * @return
     */
    public static String toJavaIdentifier(String name){
        String identifier = toIdentifier(name);
        // 如果是java保留字，那么加前缀
        if(JAVA_RESERVED_WORDS.contains(identifier)){
            return "_" + identifier;
        } else if(name.startsWith("_")){
            return "_" + identifier;
        }
        return identifier;
    }

    /**
     * 将字符串转换成合理的标示符
     * @param name
     * @return
     */
    public static String toIdentifier(String name){
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for(char c : name.toCharArray()){
            if(first && Character.isJavaIdentifierStart(c)){
                sb.append(c);
                first = false;
            } else if(!first && Character.isJavaIdentifierPart(c)){
                sb.append(c);
            } else {
                if(first && Character.isJavaIdentifierPart(c) && !Character.isJavaIdentifierStart(c)){
                    sb.append("_").append(c);
                } else {
                    if(Character.isJavaIdentifierPart(c)){
                        sb.append(c);
                    } else if(!Character.isWhitespace(c)){
                        sb.append("_");
                    }
                }
                first = false;
            }
        }
        return sb.toString();
    }
}
