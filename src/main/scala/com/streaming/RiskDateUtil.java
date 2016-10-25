package com.streaming;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by lichangyue on 2016/10/21.
 */
public class RiskDateUtil {

    private static SimpleDateFormat sp  = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+08:00");
    private static SimpleDateFormat sp2 = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

    public static  String getYesterday(){

        Calendar cal=Calendar.getInstance();
        cal.add(Calendar.DATE,-1);
        Date d=cal.getTime();

        SimpleDateFormat sp=new SimpleDateFormat("yyyyMMdd");
        String ZUOTIAN=sp.format(d);//获取昨天日期

        return ZUOTIAN;
    }

    public static  String getTodayday(){

        SimpleDateFormat sp=new SimpleDateFormat("yyyyMMdd");
        String today=sp.format(new Date());//获取昨天日期
        return today;
    }

    public static String formatDate(String date){
        if(date == null || date.trim().equals("")) return "";
        try{
            Date tt = sp.parse(date);
            return sp2.format(tt);
        }catch (Exception e){
            e.printStackTrace();
        }
        return "";
    }

}
