rootProject.name="trino-demo"

listOf("trino-ext-authz", "hive-authz").forEach { folder ->
    include(folder)
    project(":${folder}").projectDir = file(folder)
}
