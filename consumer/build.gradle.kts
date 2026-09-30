plugins {
    application
}

dependencies {
    implementation(project(":common"))
}

// The Live Order Board page lives in consumer/public/ and is served from the classpath.
sourceSets {
    main {
        resources.srcDir("public")
    }
}

application {
    applicationName = "order-consumer"
    mainClass.set("demo.consumer.ConsumerMainKt")
}
