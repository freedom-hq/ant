package at.vibing.ant.stream

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import java.io.File
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/**
 * The account key, held by the app rather than the library — the
 * Android counterpart of the iOS Keychain / Secure Enclave store.
 *
 * Swarm accounts are secp256k1, which the Android Keystore cannot hold,
 * so the Keystore holds an AES-256-GCM *wrapping* key that never leaves
 * it (hardware-backed where the device has a TEE or StrongBox), and the
 * identity document is stored encrypted under it in the app's private
 * files. The ciphertext is useless off this device; "Back up key" in the
 * Storage tab is the recovery path.
 */
object AccountKeystore {
    private const val KEY_ALIAS = "antstream.identity.wrap"
    private const val FILE_NAME = "identity.enc"

    private fun file(context: Context) = File(context.filesDir, FILE_NAME)

    /**
     * Load the stored identity, or mint and store a fresh one when there
     * is none and [allowCreate] is set. A data dir that has already run
     * an account must not silently get a new one, so the caller passes
     * `allowCreate = false` then.
     */
    fun loadOrCreate(context: Context, allowCreate: Boolean): String {
        val f = file(context)
        if (f.exists()) return decrypt(f.readText())
        if (!allowCreate) {
            throw AntException("This device's account key is gone. Restore it from your backup.")
        }
        val identity = AntNative.nativeIdentityGenerate()
        store(context, identity)
        return identity
    }

    /** Replace the stored identity (restore path). Written before use. */
    fun store(context: Context, identityJson: String) {
        val f = file(context)
        val tmp = File(f.parentFile, "$FILE_NAME.tmp")
        tmp.writeText(encrypt(identityJson))
        if (!tmp.renameTo(f)) throw AntException("could not store the account key")
    }

    fun isHardwareBacked(): Boolean = runCatching {
        val key = wrappingKey()
        val factory = javax.crypto.SecretKeyFactory.getInstance(key.algorithm, "AndroidKeyStore")
        val info = factory.getKeySpec(key, android.security.keystore.KeyInfo::class.java)
            as android.security.keystore.KeyInfo
        if (android.os.Build.VERSION.SDK_INT >= 31) {
            info.securityLevel != KeyProperties.SECURITY_LEVEL_SOFTWARE
        } else {
            @Suppress("DEPRECATION")
            info.isInsideSecureHardware
        }
    }.getOrDefault(false)

    private fun wrappingKey(): SecretKey {
        val ks = KeyStore.getInstance("AndroidKeyStore").apply { load(null) }
        (ks.getKey(KEY_ALIAS, null) as? SecretKey)?.let { return it }
        val gen = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore")
        gen.init(
            KeyGenParameterSpec.Builder(
                KEY_ALIAS,
                KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT,
            )
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setKeySize(256)
                .build(),
        )
        return gen.generateKey()
    }

    private fun encrypt(plain: String): String {
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, wrappingKey())
        val sealed = cipher.doFinal(plain.toByteArray(Charsets.UTF_8))
        return Base64.encodeToString(cipher.iv, Base64.NO_WRAP) + ":" +
            Base64.encodeToString(sealed, Base64.NO_WRAP)
    }

    private fun decrypt(stored: String): String {
        val parts = stored.trim().split(":")
        if (parts.size != 2) throw AntException("stored account key is corrupt")
        val iv = Base64.decode(parts[0], Base64.NO_WRAP)
        val sealed = Base64.decode(parts[1], Base64.NO_WRAP)
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.DECRYPT_MODE, wrappingKey(), GCMParameterSpec(128, iv))
        return String(cipher.doFinal(sealed), Charsets.UTF_8)
    }
}
