# One image for the whole demo: the minikafka broker plus the order-producer and order-consumer
# services, compiled in a single Gradle run. (Separate per-service builds would run several
# Gradle builds in parallel, which exhausts small Docker VMs such as Colima's 2 GiB default.)
#
# The broker source is minikafka's GitHub tarball at MINIKAFKA_REF (a branch, tag or commit), so
# the build context is just this directory; the services compile against that checkout's own
# client library through the same composite build used locally.

FROM eclipse-temurin:25-jdk AS build
ARG MINIKAFKA_REF=main
ARG MINIKAFKA_SOURCE=https://github.com/vishnukprakash/minikafka/archive/${MINIKAFKA_REF}.tar.gz
# Bounded heaps, one worker, and the Kotlin compiler in-process instead of a separate daemon JVM.
ENV GRADLE_OPTS="-Xmx128m"
RUN mkdir -p /root/.gradle && printf '%s\n' \
    'org.gradle.jvmargs=-Xmx768m -XX:MaxMetaspaceSize=384m' \
    'org.gradle.workers.max=1' \
    'org.gradle.daemon=false' \
    'kotlin.compiler.execution.strategy=in-process' \
    > /root/.gradle/gradle.properties

WORKDIR /src
ADD ${MINIKAFKA_SOURCE} /tmp/minikafka.tar.gz
RUN mkdir minikafka \
    && tar -xzf /tmp/minikafka.tar.gz -C minikafka --strip-components=1 \
    && rm /tmp/minikafka.tar.gz

WORKDIR /src/minikafka-demo
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
COPY common/build.gradle.kts common/
COPY producer/build.gradle.kts producer/
COPY consumer/build.gradle.kts consumer/
# Resolve Gradle and all dependencies in their own layer so source edits don't re-download them.
RUN ./gradlew --no-daemon :producer:dependencies :consumer:dependencies --configuration runtimeClasspath > /dev/null
COPY common/src common/src
COPY producer/src producer/src
COPY consumer/src consumer/src
COPY consumer/public consumer/public
RUN ./gradlew --no-daemon :minikafka:installDist installDist

FROM eclipse-temurin:25-jre
RUN useradd --system --uid 1001 --create-home demo \
    && mkdir -p /data \
    && chown demo /data
COPY --from=build /src/minikafka/build/install/minikafka /opt/minikafka
COPY --from=build /src/minikafka-demo/producer/build/install/order-producer /opt/order-producer
COPY --from=build /src/minikafka-demo/consumer/build/install/order-consumer /opt/order-consumer
ENV PATH="/opt/minikafka/bin:/opt/order-producer/bin:/opt/order-consumer/bin:${PATH}" \
    JAVA_OPTS="-Xmx256m --enable-native-access=ALL-UNNAMED"
USER demo
WORKDIR /home/demo
CMD ["minikafka", "--help"]
