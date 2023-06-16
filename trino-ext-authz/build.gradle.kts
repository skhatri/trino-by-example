
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

configurations {
    implementation {
        resolutionStrategy.failOnVersionConflict()
    }
}
val plugin by configurations.runtimeClasspath
dependencies {
    listOf("trino-spi", "trino-plugin-toolkit").forEach { name ->
        implementation("io.trino:${name}:418")
    }
    implementation("com.google.guava:guava:30.1.1-jre")
    compileOnly("org.slf4j:slf4j-api:1.7.36")

}


tasks.register<Copy>("copyDeps") {
    from(plugin)
    include("**/*.jar")
    into("$buildDir/ext")
}

tasks.build {
    dependsOn("copyDeps")
}
