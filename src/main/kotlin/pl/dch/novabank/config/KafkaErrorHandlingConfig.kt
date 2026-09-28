package pl.dch.novabank.config

import org.apache.kafka.common.TopicPartition
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff

@Configuration
class KafkaErrorHandlingConfig {

    @Bean
    fun defaultErrorHandler(
        kafkaOperations: KafkaOperations<Any, Any>,
        kafkaTopicsProperties: KafkaTopicsProperties
    ): DefaultErrorHandler {
        val recoverer = DeadLetterPublishingRecoverer(kafkaOperations) { consumerRecord, _ ->
            TopicPartition(kafkaTopicsProperties.transferRequestedDlt, consumerRecord.partition())
        }

        return DefaultErrorHandler(recoverer, FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRY_ATTEMPTS))
    }

    companion object {
        private const val RETRY_INTERVAL_MS = 1000L
        private const val MAX_RETRY_ATTEMPTS = 3L
    }
}
