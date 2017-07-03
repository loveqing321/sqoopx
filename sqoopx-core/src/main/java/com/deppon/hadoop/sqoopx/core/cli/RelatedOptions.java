package com.deppon.hadoop.sqoopx.core.cli;

import org.apache.commons.cli.Options;

/**
 * Created by meepai on 2017/6/19.
 */
public class RelatedOptions extends Options {

    private String title;

    public RelatedOptions(){}

    public RelatedOptions(String title){
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
