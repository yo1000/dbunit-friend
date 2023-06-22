package com.yo1000.dbunit.friend

class NamedCsvString(
    val name: String,
    val csv: String,
    val delimiter: Char = ',',
    val enclosure: Char = '\''
)
