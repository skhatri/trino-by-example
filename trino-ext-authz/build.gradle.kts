val trinoVersion:String by project

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

configurations {
    implementation {
        resolutionStrategy.failOnVersionConflict()
    }
}
val plugin by configurations.runtimeClasspath
dependencies {
    listOf("trino-spi", "trino-plugin-toolkit").forEach { name ->
        implementation("io.trino:${name}:$trinoVersion")
    }
    implementation("com.google.guava:guava:33.3.0-jre")
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
