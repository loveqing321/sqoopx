package com.deppon.hadoop.sqoopx.core.util.password;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.crypto.*;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

/**
 * Created by meepai on 2017/6/23.
 */
public class CryptoPasswordLoader extends FilePasswordLoader {

    private static final String PROPERTY_CRYPTO_PASS_PHRASE = "com.deppon.sqoopx.credentials.loader.crypto.passphrase";

    private static final String PROPERTY_CRYPTO_SALT = "com.deppon.sqoopx.credentials.loader.crypto.salt";

    private static final String DEFAULT_SALT = "SALT";

    private static final String PROPERTY_CRYPTO_ITERATIONS = "com.deppon.sqoopx.credentials.loader.crypto.iterations";

    private static final int DEFAULT_ITERATIONS = 10000;

    private static final String PROPERTY_CRYPTO_KEY_LENGTH = "com.deppon.sqoopx.credentials.loader.crypto.key.len";

    private static final int DEFAULT_KEY_LENGTH = 128;

    private static final String PROPERTY_CRYPTO_ALG = "com.deppon.sqoopx.credentials.loader.crypto.alg";

    private static final String DEFAULT_ALG = "AES/ECB/PKCS5Padding";

    @Override
    public String loadPassword(String p, Configuration conf) throws IOException {
        Path path = new Path(p);
        FileSystem fs = path.getFileSystem(conf);
        // verify path
        verifyPath(fs, path);
        byte[] encrypted = readBytes(fs, path);
        fs.close();

        String pass = conf.get(PROPERTY_CRYPTO_PASS_PHRASE);
        if(pass == null){
            throw new IOException("Property " + PROPERTY_CRYPTO_PASS_PHRASE + " cannot be null!");
        }
        String salt = conf.get(PROPERTY_CRYPTO_SALT, DEFAULT_SALT);
        int iterationCount = conf.getInt(PROPERTY_CRYPTO_ITERATIONS, DEFAULT_ITERATIONS);
        int keyLength = conf.getInt(PROPERTY_CRYPTO_KEY_LENGTH, DEFAULT_KEY_LENGTH);

        // 算法集合
        String algorithms = conf.get(PROPERTY_CRYPTO_ALG, DEFAULT_ALG);
        String algorithm = algorithms.split("/")[0];

        try {
            // 构建密钥规范
            KeySpec keySpec = new PBEKeySpec(pass.toCharArray(), salt.getBytes(), iterationCount, keyLength);
            // 密钥工厂
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            // 中间密钥 key
            SecretKey key = factory.generateSecret(keySpec);
            // 密钥规范
            SecretKeySpec secretKeySpec = new SecretKeySpec(key.getEncoded(), algorithm);
            // jce 加密/解密器
            Cipher cipher = Cipher.getInstance(algorithm);
            // 用密钥初始化该 cipher
            cipher.init(Cipher.DECRYPT_MODE, key);
            // 解密
            byte[] decrypted = cipher.doFinal(encrypted);
            return new String(decrypted);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e.getMessage(), e);
        } catch (InvalidKeySpecException e) {
            throw new IOException(e.getMessage(), e);
        } catch (NoSuchPaddingException e) {
            throw new IOException(e.getMessage(), e);
        } catch (InvalidKeyException e) {
            throw new IOException(e.getMessage(), e);
        } catch (BadPaddingException e) {
            throw new IOException(e.getMessage(), e);
        } catch (IllegalBlockSizeException e) {
            throw new IOException(e.getMessage(), e);
        }
    }
}
