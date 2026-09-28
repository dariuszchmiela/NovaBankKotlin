package pl.dch.novabank

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

// The poll interval is also the scheduler's initial delay, so a very long one keeps it from firing during tests;
// tests drive OutboxPublisherScheduler explicitly and stay deterministic
@SpringBootTest(properties = ["novabank.outbox.poll-interval-ms=3600000"])
abstract class AbstractIntegrationTest {

    companion object {
        @JvmStatic
        @ServiceConnection
        val postgresContainer = PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"))
            .apply { start() }

        @JvmStatic
        @ServiceConnection
        val kafkaContainer = ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .apply { start() }
    }
}
