
java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

configurations {
    implementation {
        resolutionStrategy.failOnVersionConflict()
    }
}
val plugin by configurations.runtimeClasspath
dependencies {
    listOf("trino-spi", "trino-plugin-toolkit").forEach { name ->
        implementation("io.trino:${name}:359")
    }
    implementation("com.google.guava:guava:30.1.1-jre")
}


tasks.register<Copy>("copyDeps") {
    from(plugin)
    include("**/*.jar")
    into("$buildDir/ext")
}

tasks.build {
    dependsOn("copyDeps")
}
