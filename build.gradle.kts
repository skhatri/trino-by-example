tasks.wrapper {
    gradleVersion = "8.10"
}

allprojects {
    apply(plugin = "idea")
    apply(plugin = "java")

    repositories {
        mavenCentral()
        maven {
            url = uri("https://oss.sonatype.org/content/repositories/snapshots")
        }
    }

}

