package com.streaming.utils;

import java.math.BigInteger;
import java.security.MessageDigest;

/**
 * Created by tx_hemaojie on 2015/12/21.
 */
public class EncryptUtil {

    /**
     * @Title: encrypt
     * @Description: 对称加密
     * @param @param password
     * @param @return    设定文件
     * @return String    返回类型
     * @throws
     */
    public static final String encrypt(String password,String SEED,int RADIX) {

        if(isEmpty(password)){
            return "";
        }

        BigInteger bi_passwd = new BigInteger(password.getBytes());

        BigInteger bi_r0 = new BigInteger(SEED);
        BigInteger bi_r1 = bi_r0.xor(bi_passwd);

        return bi_r1.toString(RADIX);
    }

    /**
     * @Title: decrypt
     * @Description: 对称解密
     * @param @param encrypted
     * @param @return    设定文件
     * @return String    返回类型
     * @throws
     */
    public static final String decrypt(String encrypted,String SEED,int RADIX) {

        if(isEmpty(encrypted)){
            return "";
        }

        BigInteger bi_confuse = new BigInteger(SEED);

        try {
            BigInteger bi_r1 = new BigInteger(encrypted, RADIX);
            BigInteger bi_r0 = bi_r1.xor(bi_confuse);
            return new String(bi_r0.toByteArray());
        } catch (Exception e) {
            return "";
        }

    }

    /**
     * @Title: encodeMessage
     * @Description: md5签名
     * @param @param data
     * @param @return
     * @param @throws Exception    设定文件
     * @return String    返回类型
     * @throws
     */
    public static String encodeMessage(String data) throws Exception {

        if(isEmpty(data)){
            return "";
        }

        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(data.getBytes());
        return toHex(md5.digest());

    }

    private static String toHex(byte[] buffer) {
        byte[] result = new byte[buffer.length * 2];

        for (int i = 0; i < buffer.length; i++) {
            byte[] temp = getHexValue(buffer[i]);
            result[(i * 2)] = temp[0];
            result[(i * 2 + 1)] = temp[1];
        }
        return new String(result).toUpperCase();
    }

    private static byte[] getHexValue(byte b) {
        int value = b;
        if (value < 0) {
            value = 256 + b;
        }
        String s = Integer.toHexString(value);
        if (s.length() == 1) {
            return new byte[] { 48, (byte) s.charAt(0) };
        }
        return new byte[] { (byte) s.charAt(0), (byte) s.charAt(1) };
    }

    /**
     * @Title: isEmpty
     * @Description: 判断是否为空
     * @param @param str
     * @param @return    设定文件
     * @return boolean    返回类型
     * @throws
     */
    private static boolean isEmpty(String str){
        return str == null || str.length() == 0;
    }

    public static void main(String[] args) throws Exception{
        int RADIX = 16;
        String SEED = "0933910847463829232312312";
        String content = "1234";
		/*1.对称加解密*/
        System.out.println("加密前：" + content);

        String encrypt = EncryptUtil.encrypt(content,SEED,RADIX);
        System.out.println("加密后：" + encrypt);

        String decrypt = EncryptUtil.decrypt(encrypt,SEED,RADIX);
        System.out.println("解密后：" + decrypt);

    }



}
