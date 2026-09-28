package pl.dch.novabank.config

import jakarta.validation.constraints.Positive
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated

@ConfigurationProperties(prefix = "novabank.outbox")
@Validated
data class OutboxProperties(
    @field:Positive
    val pollIntervalMs: Long,

    @field:Positive
    val publishTimeoutSeconds: Long
)
