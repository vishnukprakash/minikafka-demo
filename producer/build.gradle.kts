plugins {
    application
}

dependencies {
    implementation(project(":common"))
}

application {
    applicationName = "order-producer"
    mainClass.set("demo.producer.ProducerMainKt")
}
