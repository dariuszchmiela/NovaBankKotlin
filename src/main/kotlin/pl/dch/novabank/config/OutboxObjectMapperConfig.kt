package pl.dch.novabank.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import tools.jackson.databind.ObjectMapper
import tools.jackson.module.kotlin.jacksonObjectMapper

@Configuration
class OutboxObjectMapperConfig {

    @Bean
    fun outboxObjectMapper(): ObjectMapper = jacksonObjectMapper()
}