package pl.dch.novabank

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

@SpringBootTest
abstract class AbstractIntegrationTest {

    companion object {
        @JvmStatic
        @ServiceConnection
        val postgresContainer = PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"))
            .apply { start() }
    }
}
