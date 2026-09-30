rootProject.name = "minikafka-demo"

include("common", "producer", "consumer")

// The broker's own client library (minikafka.client.MiniKafkaClient) comes from a minikafka
// checkout via a composite build: `minikafka:minikafka` is substituted by that project.
// Default: a sibling checkout; override with -PminikafkaDir=... or MINIKAFKA_DIR.
includeBuild(
    providers.gradleProperty("minikafkaDir")
        .orElse(providers.environmentVariable("MINIKAFKA_DIR"))
        .getOrElse("../minikafka")
)
