package pl.dch.novabank

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class NovaBankKotlinApplication

fun main(args: Array<String>) {
    runApplication<NovaBankKotlinApplication>(*args)
}
