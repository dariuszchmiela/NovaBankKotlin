package pl.dch.novabank.service

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import pl.dch.novabank.AbstractIntegrationTest
import pl.dch.novabank.config.KafkaTopicsProperties
import pl.dch.novabank.event.TransferRequestedEvent
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class TransferConsumerDltIntegrationTest(
    @param:Autowired private val kafkaTemplate: KafkaTemplate<Any, Any>,
    @param:Autowired @param:Qualifier("outboxKafkaTemplate") private val rawKafkaTemplate: KafkaTemplate<String, String>,
    @param:Autowired private val kafkaTopicsProperties: KafkaTopicsProperties
) : AbstractIntegrationTest() {

    private val dltConsumer = KafkaConsumer<String, String>(
        mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "transfer-dlt-it-${UUID.randomUUID()}",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
        )
    ).apply { subscribe(listOf(kafkaTopicsProperties.transferRequestedDlt)) }

    @AfterEach
    fun tearDownDltConsumer() {
        dltConsumer.close()
    }

    @Test
    fun `should publish to DLT after processing keeps failing`() {
        val key = "ACC-SOURCE-DLT-IT-${UUID.randomUUID()}"
        val invalidEvent = TransferRequestedEvent(
            transferId = INVALID_TRANSFER_ID,
            sourceAccountId = key,
            targetAccountId = "ACC-TARGET-DLT-IT",
            amount = BigDecimal("100.00"),
            currency = "PLN",
            requestedAt = Instant.now()
        )

        kafkaTemplate.send(kafkaTopicsProperties.transferRequested, key, invalidEvent).get()

        val dltRecord = pollDltRecordWithKey(key)
        assertTrue(dltRecord.value().contains(INVALID_TRANSFER_ID))
        assertTrue(dltExceptionHeaders(dltRecord).contains(IllegalArgumentException::class.java.name))
    }

    @Test
    fun `should publish original bytes to DLT when value cannot be deserialized`() {
        val key = "ACC-SOURCE-DLT-DESER-IT-${UUID.randomUUID()}"

        rawKafkaTemplate.send(kafkaTopicsProperties.transferRequested, key, MALFORMED_PAYLOAD).get()

        val dltRecord = pollDltRecordWithKey(key)
        assertEquals(MALFORMED_PAYLOAD, dltRecord.value())
        assertTrue(dltExceptionHeaders(dltRecord).contains("DeserializationException"))
    }

    private fun dltExceptionHeaders(record: ConsumerRecord<String, String>): String =
        listOf(KafkaHeaders.DLT_EXCEPTION_FQCN, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)
            .mapNotNull { record.headers().lastHeader(it)?.value()?.decodeToString() }
            .joinToString()

    private fun pollDltRecordWithKey(key: String): ConsumerRecord<String, String> {
        val deadline = Instant.now().plus(POLL_TIMEOUT)

        while (Instant.now().isBefore(deadline)) {
            dltConsumer.poll(Duration.ofMillis(500))
                .records(kafkaTopicsProperties.transferRequestedDlt)
                .firstOrNull { it.key() == key }
                ?.let { return it }
        }

        fail("No record found on DLT with key $key")
    }

    companion object {
        private const val INVALID_TRANSFER_ID = "not-a-valid-uuid"
        private const val MALFORMED_PAYLOAD = "{this is not valid json"
        private val POLL_TIMEOUT = Duration.ofSeconds(30)
    }
}
