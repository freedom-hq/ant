// AntStream app module. The native lib (`crates/ant-ffi`, features
// `jni,chain`) is cross-compiled out-of-band by
// `cargo xtask build-android-<abi> --features chain --app android-stream`,
// which drops `libant_ffi.so` into `app/src/main/jniLibs/<abi>/`.

plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.compose)
}

android {
    namespace = "at.vibing.ant.stream"
    compileSdk = libs.versions.compileSdk.get().toInt()

    defaultConfig {
        // Same reverse-DNS as the iOS bundle id.
        applicationId = "at.vibing.ant.stream"
        minSdk = libs.versions.minSdk.get().toInt()
        targetSdk = libs.versions.targetSdk.get().toInt()
        versionCode = (findProperty("antstream.versionCode") as String?)?.toInt() ?: 1
        versionName = (findProperty("antstream.versionName") as String?) ?: "0.1.0"

        ndk {
            //noinspection ChromeOsAbiSupport
            abiFilters += listOf("arm64-v8a", "x86_64")
        }
    }

    signingConfigs {
        // A fixed, *public* example key, so every CI build installs over
        // the previous one and keeps the app's data (the account key
        // lives in it). Its password is in this file on purpose: this is
        // a sample app, not a store release. A real release would sign
        // with a private key passed in through the environment.
        create("example") {
            storeFile = file("antstream-example.keystore")
            storePassword = "antstream"
            keyAlias = "antstream"
            keyPassword = "antstream"
        }
    }

    buildTypes {
        getByName("release") {
            isMinifyEnabled = true
            isShrinkResources = true
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro",
            )
            signingConfig = signingConfigs.getByName("example")
        }
        getByName("debug") {
            isMinifyEnabled = false
            signingConfig = signingConfigs.getByName("example")
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    buildFeatures {
        compose = true
        buildConfig = false
    }

    packaging {
        resources {
            excludes += setOf(
                "META-INF/AL2.0",
                "META-INF/LGPL2.1",
                "/META-INF/{AL2.0,LGPL2.1}",
            )
        }
        jniLibs {
            useLegacyPackaging = false
        }
    }
}

dependencies {
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.lifecycle.viewmodel.compose)
    implementation(libs.androidx.activity.compose)

    val composeBom = platform(libs.androidx.compose.bom)
    implementation(composeBom)

    implementation(libs.androidx.compose.ui)
    implementation(libs.androidx.compose.ui.graphics)
    implementation(libs.androidx.compose.ui.tooling.preview)
    implementation(libs.androidx.compose.material3)

    implementation(libs.androidx.media3.exoplayer)
    implementation(libs.androidx.media3.exoplayer.hls)
    implementation(libs.androidx.media3.ui)
    implementation(libs.zxing.core)

    debugImplementation(libs.androidx.compose.ui.tooling)

    testImplementation("junit:junit:4.13.2")
}
