package com.yo1000.dbunit.friend

import org.dbunit.dataset.CachedDataSet
import org.dbunit.dataset.IDataSet
import org.dbunit.dataset.ReplacementDataSet

class NamedCsvStringDataSet(
    namedCsvStrings: List<NamedCsvString>
) : CachedDataSet(NamedCsvStringProducer(namedCsvStrings)) {
    companion object {
        @JvmStatic
        fun builder(): Builder {
            return Builder(delimiter = '|', enclosure = '\'')
        }

        @JvmStatic
        fun builder(delimiter: Char, enclosure: Char): Builder {
            return Builder(delimiter = delimiter, enclosure = enclosure)
        }
    }

    class Builder(
        private val delimiter: Char = '|',
        private val enclosure: Char = '\''
    ) {
        private val tables: MutableList<String> = mutableListOf()
        private val csvs: MutableList<String> = mutableListOf()

        fun table(name: String): Builder {
            tables += name
            return this
        }

        fun csv(colsWithData: String): Builder {
            csvs += colsWithData.trimIndent()
            return this
        }

        fun build(): IDataSet {
            val namedCsvStrings: MutableList<NamedCsvString> = mutableListOf()
            val count = if (tables.size >= csvs.size) tables.size else csvs.size

            (0 until count).forEach { i ->
                namedCsvStrings += NamedCsvString(
                    name = tables[i],
                    csv = csvs[i],
                    delimiter = delimiter
                )
            }

            return NamedCsvStringDataSet(namedCsvStrings).let {
                ReplacementDataSet(it).also { wrapped ->
                    wrapped.addReplacementObject("", null)
                    wrapped.addReplacementObject("${enclosure}${enclosure}", "")
                }
            }
        }
    }
}
