package pl.dch.novabank.repository

import org.springframework.data.jpa.repository.JpaRepository
import pl.dch.novabank.entity.ProcessedTransfer
import java.util.UUID

interface ProcessedTransferRepository : JpaRepository<ProcessedTransfer, UUID>
