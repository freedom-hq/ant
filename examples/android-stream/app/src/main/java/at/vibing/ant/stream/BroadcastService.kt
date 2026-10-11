package at.vibing.ant.stream

import android.annotation.SuppressLint
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.wifi.WifiManager
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import kotlinx.coroutines.launch

/**
 * Keeps a broadcast alive while the app is not on screen: a foreground
 * service of type camera + microphone (so the OS lets capture continue),
 * a partial wake lock (so encoding and publishing continue with the
 * screen off) and a Wi-Fi lock. The notification's Stop action ends the
 * broadcast the same way the Stop button does.
 *
 * Started by [LiveBroadcast] when a broadcast goes live, stopped when it
 * ends. It holds no state of its own.
 */
class BroadcastService : Service() {
    private var wakeLock: PowerManager.WakeLock? = null
    private var wifiLock: WifiManager.WifiLock? = null

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            val app = application as AntStreamApp
            app.appScope.launch { app.broadcast.stop() }
            return START_NOT_STICKY
        }
        val types = if (Build.VERSION.SDK_INT >= 30) {
            var t = ServiceInfo.FOREGROUND_SERVICE_TYPE_CAMERA
            if (intent?.getBooleanExtra(EXTRA_AUDIO, false) == true) t = t or ServiceInfo.FOREGROUND_SERVICE_TYPE_MICROPHONE
            t
        } else {
            0
        }
        ServiceCompat.startForeground(this, NOTIFICATION_ID, notification(intent?.getStringExtra(EXTRA_CHANNEL)), types)
        acquireLocks()
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        wakeLock?.takeIf { it.isHeld }?.release()
        wifiLock?.takeIf { it.isHeld }?.release()
        super.onDestroy()
    }

    @SuppressLint("WakelockTimeout")
    private fun acquireLocks() {
        if (wakeLock == null) {
            wakeLock = getSystemService(PowerManager::class.java)
                ?.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "antstream:broadcast")
                ?.apply { acquire() }
        }
        if (wifiLock == null) {
            @Suppress("DEPRECATION")
            val mode = if (Build.VERSION.SDK_INT >= 29) WifiManager.WIFI_MODE_FULL_LOW_LATENCY else WifiManager.WIFI_MODE_FULL_HIGH_PERF
            wifiLock = applicationContext.getSystemService(WifiManager::class.java)
                ?.createWifiLock(mode, "antstream:broadcast")
                ?.apply { acquire() }
        }
    }

    private fun notification(channel: String?): Notification {
        val nm = getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= 26 && nm.getNotificationChannel(CHANNEL_ID) == null) {
            nm.createNotificationChannel(
                NotificationChannel(CHANNEL_ID, "Live broadcast", NotificationManager.IMPORTANCE_LOW),
            )
        }
        val open = PendingIntent.getActivity(
            this, 0,
            Intent(this, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP),
            PendingIntent.FLAG_IMMUTABLE,
        )
        val stop = PendingIntent.getService(
            this, 1,
            Intent(this, BroadcastService::class.java).setAction(ACTION_STOP),
            PendingIntent.FLAG_IMMUTABLE,
        )
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_launcher)
            .setContentTitle("Live on Swarm")
            .setContentText(channel?.let { "Broadcasting to $it" } ?: "Broadcasting")
            .setOngoing(true)
            .setContentIntent(open)
            .addAction(0, "Stop", stop)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .build()
    }

    companion object {
        private const val CHANNEL_ID = "broadcast"
        private const val NOTIFICATION_ID = 1
        private const val ACTION_STOP = "at.vibing.ant.stream.STOP"
        private const val EXTRA_CHANNEL = "channel"
        private const val EXTRA_AUDIO = "audio"

        fun start(context: Context, channel: String, withAudio: Boolean) {
            val intent = Intent(context, BroadcastService::class.java)
                .putExtra(EXTRA_CHANNEL, channel)
                .putExtra(EXTRA_AUDIO, withAudio)
            if (Build.VERSION.SDK_INT >= 26) context.startForegroundService(intent) else context.startService(intent)
        }

        fun stop(context: Context) {
            context.stopService(Intent(context, BroadcastService::class.java))
        }
    }
}
