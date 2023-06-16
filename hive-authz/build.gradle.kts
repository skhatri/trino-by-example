

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

configurations {
    implementation {
        resolutionStrategy.failOnVersionConflict()
    }
}

dependencies {
    listOf("hive-service").forEach { name ->
        implementation("org.apache.hive:${name}:3.1.2")
    }
    listOf("aws-java-sdk-sts", "aws-java-sdk-s3", "aws-java-sdk-bom").forEach { mod ->
        compileOnly("com.amazonaws:${mod}:1.11.860")
    }
    listOf("trino-spi", "trino-plugin-toolkit", "trino-hive").forEach { name ->
        compileOnly("io.trino:${name}:418")
    }
    compileOnly("org.slf4j:slf4j-api:1.7.36")
}

