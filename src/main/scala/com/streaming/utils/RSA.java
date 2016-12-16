package com.streaming.utils;

import com.streaming.exception.RSAAlgorithmException;
import com.streaming.exception.RSAGenerateKeyException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import javax.crypto.Cipher;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * RSA加密解密
 * @author sunhangda
 */
public class RSA {
	
	/**
	 * 可以更改，但是不要太大，否则效率会低
	 */
	public static final int KEY_SIZE = 512;
	/**
	 * 生成密钥对
	 * @return
	 * @throws RSAGenerateKeyException
	 */
	public static KeyPair generateKeyPair() throws RSAGenerateKeyException {
		try {
			KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
			keyPairGen.initialize(KEY_SIZE);
			java.security.KeyPair kp = keyPairGen.generateKeyPair();
			return transfer(kp);
		} catch (Exception e) {
			throw new RSAGenerateKeyException("密钥对生成异常", e);
		}
	}
	/**
	 * 转换方法
	 * @param kp
	 * @return
	 */
	private static KeyPair transfer(java.security.KeyPair kp){
		KeyPair keyPair = new KeyPair();
		//转成16进制数字
        String publicKey = Base64.encodeBase64String(kp.getPublic().getEncoded());
        String privateKey = Base64.encodeBase64String(kp.getPrivate().getEncoded());
        keyPair.setPublicKey(publicKey);
        keyPair.setPrivateKey(privateKey);
		return keyPair;
	}
	/**
	 * 生成公钥对象
	 * @return
	 * @throws RSAGenerateKeyException
	 */
	private static PublicKey generateRSAPublicKey(String publicKey) throws RSAGenerateKeyException {
		try {
			X509EncodedKeySpec keySpec = new X509EncodedKeySpec(Base64.decodeBase64(publicKey));
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePublic(keySpec);
		} catch (Exception e) {
			throw new RSAGenerateKeyException("公钥生成异常", e);
		}
	}
	/**
	 * 生成私钥对象
	 * @return
	 * @throws RSAGenerateKeyException
	 */
	private static PrivateKey generateRSAPrivateKey(String privateKey) throws RSAGenerateKeyException {
		try {
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(Base64.decodeBase64(privateKey));
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(keySpec);
		} catch (Exception e) {
			throw new RSAGenerateKeyException("私钥生成异常", e);
		}
	}
	/**
	 * RSA加密方法
	 * @param publicKey
	 * @param text 明文
	 * @return 16进制
	 * @throws RSAAlgorithmException
	 */
	public static String encrypt(String publicKey, String text) throws RSAAlgorithmException {
		try {
			Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
			cipher.init(Cipher.ENCRYPT_MODE, generateRSAPublicKey(publicKey));
			byte[] buff = cipher.doFinal(text.getBytes());
			return Hex.encodeHexString(buff);
		} catch (Exception e) {
			throw new RSAAlgorithmException("加密异常", e);
		}
	}
	/**
	 * RSA解密方法
	 * @param privateKey
	 * @param text 
	 * @return
	 * @throws RSAAlgorithmException
	 */
	public static String decrypt(String privateKey, String text) throws RSAAlgorithmException {
		try {
			Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
			cipher.init(Cipher.DECRYPT_MODE, generateRSAPrivateKey(privateKey));
			byte[] buff = Hex.decodeHex(text.toCharArray());
			return new String(cipher.doFinal(buff));
		} catch (Exception e) {
			throw new RSAAlgorithmException("解密异常", e);
		}
	}
	/**
	 * 公私钥 holder类
	 * @author sunhangda
	 */
	public static class KeyPair{
		
		private String publicKey;
		private String privateKey;
		
		public String getPublicKey() {
			return publicKey;
		}
		public void setPublicKey(String publicKey) {
			this.publicKey = publicKey;
		}
		public String getPrivateKey() {
			return privateKey;
		}
		public void setPrivateKey(String privateKey) {
			this.privateKey = privateKey;
		}
		
	}
	public static void main(String[] args) throws Exception {
		KeyPair keyPair = RSA.generateKeyPair();
		System.out.println("生成公钥：" + keyPair.getPublicKey().getBytes().length);
		System.out.println("生成私钥：" + keyPair.getPrivateKey().getBytes().length);
		String text = "device23423486";
//		String encryptedData = encrypt("MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAI20syvHRIHFNj87VcoiAfQYwg4MI6TeINx9eVKtg8cNdocikWQ15kbl048WhKnwBOGlU8yKGDNGiS1K6R4cKgMCAwEAAQ==", text);
		String encryptedData = "02773c3f57d2e0273177ce6307d6a00e1338a55af7e4fc445402a5cae58691ddf18ad1558d3dcf59a4af9cee021801b531962f903c6bc9ee2c76e144f38c053b";
		System.out.println("密文：" + encryptedData);
		System.out.println("明文：" + decrypt("MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAjbSzK8dEgcU2PztVyiIB9BjCDgwjpN4g3H15Uq2Dxw12hyKRZDXmRuXTjxaEqfAE4aVTzIoYM0aJLUrpHhwqAwIDAQABAkAgcbf4FQalY57Y+V/aCNFDrwt3JeZfUBBcC0pk2J9sSlwbQ++SN778dvK+yhZ8Q9jrDVVIo9V+iZSpHdqXjKzRAiEA0+iNkWy8CLY+PAsL3iEFU8W7LI9P8RRznXywqu1bNwsCIQCrMMKBGUBZdVL3P9oKAuoF+Nmdjrm3KPCWGZ8CBsvT6QIgRK5V2/FrDEPM7fcClK8NI/atUKbuWQuw4TU9qVievLsCIBY+AJeLc1vsLXpodmjklglumr+o4qJUlGW8MHev8F25AiBe3cgLML5cWgrDCGQhBJNtJalNw9ETzay3pGMRCXYc1Q==", encryptedData));
		//以下是和页面一起做：  页面js加密试验，  java代码解密
		//页面上公钥：MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCSd61qzB2O7yV78P40T7W5ckFqOim6kH2e/GXMmjCOuPwOXJu3PeFuJ4jUdPZtBGKuLQkPLAX+ifkG0SKsjJN8ozjrN+esIstyhY3QhHSSABdjeLyOF85p/Zg2G7Ig0uelT/rAWehQFc6C9k9TR7cZBV9EDQfbkTn7u++UrhGctwIDAQAB
		//下面解密方法的私钥和上面是对应的，第二个参数是密文
	}
}
