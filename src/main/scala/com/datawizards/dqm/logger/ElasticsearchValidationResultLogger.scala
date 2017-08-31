package com.datawizards.dqm.logger

import com.datawizards.dqm.result.InvalidRecord
import com.datawizards.esclient.repository.ElasticsearchRepositoryImpl

class ElasticsearchValidationResultLogger(esUrl: String, indexName: String) extends ValidationResultLogger {
  private lazy val esRepository = new ElasticsearchRepositoryImpl(esUrl)

  override protected def logInvalidRecords(invalidRecords: Seq[InvalidRecord]): Unit = {
    for(record <- invalidRecords)
      esRepository.index(
        indexName = indexName,
        typeName = "invalid_record",
        documentId = java.util.UUID.randomUUID().toString,
        document = record
      )
  }

}
