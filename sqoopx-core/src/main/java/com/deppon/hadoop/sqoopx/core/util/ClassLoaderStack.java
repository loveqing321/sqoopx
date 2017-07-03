package com.deppon.hadoop.sqoopx.core.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by meepai on 2017/6/26.
 */
public class ClassLoaderStack {

    /**
     * @param jarFile
     * @param testClassName
     * @return
     * @throws IOException
     */
    public static ClassLoader addJarFile(String jarFile, String testClassName) throws IOException {
        ClassLoader prevClassLoader = Thread.currentThread().getContextClassLoader();
        if(testClassName != null){
            try {
                Class.forName(testClassName, true, prevClassLoader);
                return prevClassLoader;
            } catch (ClassNotFoundException e) {
            }
        }
        String jarPath = "jar:" + new File(jarFile).toURI().toURL() + "!/";
        URLClassLoader cl = URLClassLoader.newInstance(new URL[]{new URL(jarPath)}, prevClassLoader);
        try {
            if(testClassName != null) {
                Class.forName(testClassName, true, cl);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not load jar " + jarFile + " into JVM.");
        }
        Thread.currentThread().setContextClassLoader(cl);
        return prevClassLoader;
    }

    /**
     * @param classLoader
     */
    public static void setCurrentClassLoader(ClassLoader classLoader){
        Thread.currentThread().setContextClassLoader(classLoader);
    }
}
