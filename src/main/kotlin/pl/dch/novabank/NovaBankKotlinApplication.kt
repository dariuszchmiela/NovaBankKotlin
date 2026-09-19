package pl.dch.novabank

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class NovaBankKotlinApplication

fun main(args: Array<String>) {
    runApplication<NovaBankKotlinApplication>(*args)
}
