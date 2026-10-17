package at.vibing.ant.stream

import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.ImageDecoder
import android.net.Uri
import android.os.Build
import java.io.ByteArrayOutputStream

/**
 * Stream thumbnails (issue #220): a JPEG about [THUMBNAIL_LONG_SIDE] px on
 * its long side at quality [THUMBNAIL_JPEG_QUALITY], a few tens of KB,
 * from a camera frame or a photo the broadcaster picked.
 */
object Thumbnail {
    /** `bitmap` scaled down to the thumbnail size and JPEG-encoded. */
    fun encode(bitmap: Bitmap): ByteArray {
        val (w, h) = thumbnailSize(bitmap.width, bitmap.height)
        val scaled = if (w == bitmap.width && h == bitmap.height) bitmap else Bitmap.createScaledBitmap(bitmap, w, h, true)
        val out = ByteArrayOutputStream()
        scaled.compress(Bitmap.CompressFormat.JPEG, THUMBNAIL_JPEG_QUALITY, out)
        if (scaled !== bitmap) scaled.recycle()
        return out.toByteArray()
    }

    /**
     * The picked photo at `uri` as a thumbnail JPEG, decoded near the
     * target size and turned upright (ImageDecoder applies the EXIF
     * orientation; before Android 9 the photo is taken as stored).
     * Blocking.
     */
    fun fromUri(context: Context, uri: Uri): ByteArray {
        val bitmap = if (Build.VERSION.SDK_INT >= 28) {
            ImageDecoder.decodeBitmap(ImageDecoder.createSource(context.contentResolver, uri)) { decoder, info, _ ->
                val (w, h) = thumbnailSize(info.size.width, info.size.height)
                decoder.setTargetSize(w, h)
                decoder.allocator = ImageDecoder.ALLOCATOR_SOFTWARE
            }
        } else {
            val bounds = BitmapFactory.Options().apply { inJustDecodeBounds = true }
            context.contentResolver.openInputStream(uri)?.use { BitmapFactory.decodeStream(it, null, bounds) }
            var sample = 1
            while (maxOf(bounds.outWidth, bounds.outHeight) / (sample * 2) >= THUMBNAIL_LONG_SIDE) sample *= 2
            context.contentResolver.openInputStream(uri)?.use {
                BitmapFactory.decodeStream(it, null, BitmapFactory.Options().apply { inSampleSize = sample })
            }
        } ?: throw AntException("could not read the photo")
        return encode(bitmap).also { bitmap.recycle() }
    }
}
