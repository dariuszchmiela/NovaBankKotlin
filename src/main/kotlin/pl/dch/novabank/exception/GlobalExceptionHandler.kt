package pl.dch.novabank.exception

import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException::class)
    fun handleValidationException(exception: MethodArgumentNotValidException): ProblemDetail {
        log.warn("Validation failed: {}", exception.message)

        return ProblemDetail.forStatus(HttpStatus.BAD_REQUEST).apply {
            title = "Validation failed"
            detail = "One or more fields are invalid"
            setProperty("errors", extractFieldErrors(exception))
        }
    }

    // groupBy keeps encounter order, so first() preserves the first message per field
    private fun extractFieldErrors(exception: MethodArgumentNotValidException): Map<String, String> =
        exception.bindingResult.fieldErrors
            .groupBy { it.field }
            .mapValues { (_, errors) -> errors.first().defaultMessage ?: "Invalid value" }

    companion object {
        private val log = LoggerFactory.getLogger(GlobalExceptionHandler::class.java)
    }
}
