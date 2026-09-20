package pl.dch.novabank.config

import jakarta.validation.constraints.NotBlank
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated

@ConfigurationProperties(prefix = "novabank.kafka.topics")
@Validated
data class KafkaTopicsProperties(
    @field:NotBlank
    val transferRequested: String,

    @field:NotBlank
    val transferRequestedDlt: String
)