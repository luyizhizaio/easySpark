package com.streaming;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by lichangyue on 2016/10/21.
 */
public class mobileParseUtil {



    private static final String SEED = "011234";
    private static final int RADIX = 10;


    public static String evaluate(String mobile){
        String me = "";
        if(StringUtils.isBlank(mobile)){
            return me;
        }
        if(StringUtils.length(mobile) <=11){
            me = mobile;
            return me;
        }
        String text = StringUtils.substring(mobile, 3, -4);
        String middle = EncryptUtil.decrypt(text, SEED, RADIX);
        StringUtils.replace(mobile, text, middle);
        me = StringUtils.replace(mobile, text, middle);
        return me;
    }
}
