package com.atguigu.gmall.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtil {

    public static String getProperty(String fileName, String param) throws IOException {
        InputStream is = PropertyUtil.class.getClassLoader ().getResourceAsStream (fileName);
        Properties prop = new Properties ();
        prop.load (is);

        return prop.getProperty (param);

    }


}
