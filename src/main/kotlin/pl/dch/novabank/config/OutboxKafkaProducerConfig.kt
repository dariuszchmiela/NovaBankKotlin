package pl.dch.novabank.config

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate

@Configuration
class OutboxKafkaProducerConfig(
    private val kafkaProperties: KafkaProperties,
    private val kafkaConnectionDetails: KafkaConnectionDetails
) {

    // Defining any KafkaTemplate backs off Boot's auto-configured one, so the general-purpose template is declared here
    @Bean
    @Primary
    fun kafkaTemplate(): KafkaTemplate<Any, Any> =
        KafkaTemplate(DefaultKafkaProducerFactory(buildBaseProperties()))

    // Outbox payload is already serialized JSON, so it is sent as a plain String
    @Bean
    fun outboxKafkaTemplate(): KafkaTemplate<String, String> {
        val properties = buildBaseProperties().apply {
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }
        return KafkaTemplate(DefaultKafkaProducerFactory(properties))
    }

    private fun buildBaseProperties(): MutableMap<String, Any> =
        kafkaProperties.buildProducerProperties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionDetails.bootstrapServers)
        }
}
