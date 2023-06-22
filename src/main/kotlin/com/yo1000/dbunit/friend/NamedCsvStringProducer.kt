package com.yo1000.dbunit.friend

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import org.dbunit.dataset.Column
import org.dbunit.dataset.DefaultTableMetaData
import org.dbunit.dataset.csv.CsvParserImpl
import org.dbunit.dataset.datatype.DataType
import org.dbunit.dataset.stream.IDataSetConsumer
import org.dbunit.dataset.stream.IDataSetProducer
import java.io.StringReader

class NamedCsvStringProducer(
    private val namedCsvStrings: List<NamedCsvString>
) : IDataSetProducer {
    private var consumer: IDataSetConsumer? = null

    override fun setConsumer(consumer: IDataSetConsumer?) {
        this.consumer = consumer
    }

    override fun produce() {
        consumer?.let { cons ->
            cons.startDataSet()

            for (namedCsvString in namedCsvStrings) {
                val format = CSVFormat.newFormat(namedCsvString.delimiter)
                var startedTable = false
                for ((lineNumber, record: CSVRecord) in format.parse(StringReader(namedCsvString.csv)).withIndex()) {
                    if (lineNumber == 0) {
                        cons.startTable(DefaultTableMetaData(
                            namedCsvString.name,
                            record.values().map { col -> Column(col.trim(), DataType.UNKNOWN) }.toTypedArray()
                        ))
                        startedTable = true
                    } else {
                        cons.row(CsvParserImpl().parse(record
                            .values()
                            .map { it.trim() }
                            .map { it.replace(
                                Regex("^${namedCsvString.enclosure}${namedCsvString.enclosure}$"),
                                "${namedCsvString.enclosure}${namedCsvString.enclosure}${namedCsvString.enclosure}${namedCsvString.enclosure}"
                            ) }
                            .map { it.replace(Regex("^${namedCsvString.enclosure}"), "\"") }
                            .map { it.replace(Regex("${namedCsvString.enclosure}$"), "\"") }
                            .joinToString(separator = ",")
                        ).toTypedArray())
                    }
                }
                if (startedTable) cons.endTable()
            }

            cons.endDataSet()
        } ?: throw NullPointerException()
    }
}
