package com.deppon.hadoop.sqoopx.core.tools;

/**
 * 工具描述类
 * Created by meepai on 2017/6/19.
 */
public abstract class ToolDesc {

    private String name;

    private Class<? extends SqoopxTool> clazz;

    private String desc;

    public ToolDesc(){}

    public ToolDesc(String name, Class<? extends SqoopxTool> clazz, String desc){
        this.name = name;
        this.clazz = clazz;
        this.desc = desc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Class<? extends SqoopxTool> getClazz() {
        return clazz;
    }

    public void setClazz(Class<? extends SqoopxTool> clazz) {
        this.clazz = clazz;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
