package tech.oleks.eveonline.esi2ddl

import com.jayway.jsonpath.*
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.ddlutils.PlatformFactory
import org.apache.ddlutils.model.Column
import org.apache.ddlutils.model.Database
import org.apache.ddlutils.model.Table
import org.apache.ddlutils.model.TypeMap
import java.net.URL

import java.lang.RuntimeException
import java.util.concurrent.TimeUnit

/**
 * @author alexm
 */
class Esi2ddl(val swagger: String,
              val url: String,
              val dry: Boolean,
              val username: String,
              val password: String?,
              val schema: String,
              val verbose: Boolean) {

    private lateinit var jsonModels: DocumentContext
    private lateinit var esiDatabase: Database

    fun run () {
        step1ParseSwagger()
        step2BuildEsiTableModel()
        if (!dry)
            step3UploadDdl()
    }

    private fun step1ParseSwagger() {
        val start = System.nanoTime()
        URL(swagger).openStream()
            .use {
                val conf = Configuration.builder()
                    .options(Option.SUPPRESS_EXCEPTIONS)
                    .build()
                jsonModels = JsonPath.using(conf).parse(it)
            }
        println("step1ParseSwagger: ${TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)}ms")
    }

    // A creature from hell starts here
    private fun step2BuildEsiTableModel() {
        val start = System.nanoTime()
        esiDatabase = Database()
        esiDatabase.name = jsonModels.read("info.description")
        esiDatabase.version = jsonModels.read("info.version")

        /*   PATH PARAM DEFINITIONS */
        val pds = HashMap<String, Column>()
        for (jpc in jsonModels.read<List<Map<String, String>>>("\$.parameters..[?(@.in == 'path')]")) {
            val pc = Column()
            pc.name = jpc["name"]
            pc.description = jpc["description"]
            pc.type = toDatabaseType(jpc["type"], jpc["format"])
            pc.isRequired = true
            pds[pc.name] = pc
        }
        /*   TABLES    */
        for (path in jsonModels.read<Map<String, Any>>("paths").keys) {
            if (verbose) println("parsing paths.$path")
            val jT = jsonModels.read<Map<String, String>>("paths.$path.get")
            jT ?: continue // nothing to fetch
            val table = Table()
            // don't ask me why (I would not remember)
            table.name = path.replace(Regex("/\\{|\\}/|/"), "_").trim('_')
            table.description = jT["description"]
            esiDatabase.addTable(table)

            convertTableStructure("paths.$path.get.responses.200.schema", "", table)

            // path params references
            jsonModels.read<List<String>>("paths.$path.get.parameters..\$ref")?.forEachIndexed { idx, pr ->
                val name = pr.replace("#/parameters/", "")
                if (pds.containsKey(name)
                    && table.columns.find{it.name == name} == null) {
                    table.addColumn(idx, pds[name]!!.clone() as Column)
                }
            }
            // Path parameters objects
            jsonModels.read<List<Map<String, String>>>("paths.$path.get.parameters[?(@.in == 'path')]")?.forEachIndexed { idx, jp ->
                val name = jp["name"]
                if (table.columns.find{it.name == name} == null) {
                    convertSingleColumn(name!!, jp, true, table, idx)
                }
            }
        }
        // Normalize and Init
        esiDatabase.name = "\"${esiDatabase.name}\""
        esiDatabase.tables.forEach {
            it.name = "\"${it.name}\""
            it.schema = "\"$schema\""
            it.columns.forEach { col ->
                col.name = "\"${col.name}\""
            }
        }
        esiDatabase.initialize()
        println("step2BuildEsiTableModel: ${TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)}ms")
    }

    private fun convertTableStructure(path: String, name: String, table: Table) {
        val jtype = jsonModels.read<String>("$path.type")
        when (jtype) {
            "object" -> {
                val required = jsonModels.read<List<String>>("$path.required")?.toHashSet()
                for (jColKV in jsonModels.read<Map<String, Map<String, String>>>("$path.properties")) {
                    val cn = jColKV.key
                    val jCol = jColKV.value
                    // flat out inner objects
                    if (jCol["type"] == "object") {
                        convertTableStructure("$path.properties.$cn", "$name${cn}_", table)
                    }
                    else {
                        convertSingleColumn("$name$cn", jCol, required?.contains(jColKV.key) ?: false, table)
                    }
                }
            }
            // I am still confused with this type, but seems like the most logical step would be passing through it
            "array" -> {
                convertTableStructure("$path.items", name, table)
            }
            // single column response
            "integer", "number" -> {
                convertSingleColumn("$name${table.name}", jsonModels.read<Map<String, String>>(path), false, table)
            }
            else -> throw RuntimeException("Unknown model type: $jtype")
        }
    }

    private fun convertSingleColumn(name: String, jCol: Map<String, Any>, required: Boolean, table: Table, idx: Int = table.columnCount) {
        val column = Column()
        column.clone()
        column.type = toDatabaseType(jCol["type"]?.toString(), jCol["format"]?.toString())
        column.size = "4000"
        column.name = to29CharString(name)
        column.description = jCol["description"]?.toString()
        column.isRequired = required
        column.isPrimaryKey = jCol["uniqueItems"] as Boolean? ?: false
        if (verbose) println("${table.name}.${column.name}, type: ${column.type}, primary: ${column.isPrimaryKey} required: ${column.isRequired}, description: ${column.description}")
        table.addColumn(idx, column)
    }

    private fun to29CharString(s: String): String {
        if (s.length <= 29) return s
        val h = Integer.toHexString(s.hashCode())
        val cut = s.length - 29 + h.length
        return s.substring(0, (s.length - cut) / 2) + h + s.substring((s.length + cut) / 2)
    }

    private fun toDatabaseType(type: String?, format: String?): String {
        return when (type) {
            "string" ->
                when (format) {
                    "date" -> TypeMap.DATE
                    "date-time" -> TypeMap.TIMESTAMP
                    null -> {
                        TypeMap.VARCHAR
                    }
                    else -> throw RuntimeException("unknown format: $format, type: $type")
                }
            "integer" ->
                when (format) {
                    "int32", null -> TypeMap.INTEGER
                    "int64" -> TypeMap.BIGINT
                    else -> throw RuntimeException("unknown format: $format, type: $type")
                }
            "boolean" -> TypeMap.BOOLEAN
            "number" ->
                when (format) {
                    "float" -> TypeMap.FLOAT
                    "double" -> TypeMap.DOUBLE
                    else -> throw RuntimeException("unknown format: $format, type: $type")
                }
            "array" -> TypeMap.VARCHAR
            else -> throw RuntimeException("unknown type: $type")
        }
    }

    private fun step3UploadDdl() {
        val start = System.nanoTime()
        var dataSource = BasicDataSource()
        dataSource.driverClassName = "org.postgresql.Driver"
        dataSource.username = username
        if (password != null) dataSource.password = password
        dataSource.maxIdle = 2
        dataSource.maxWaitMillis = 5000
        dataSource.maxOpenPreparedStatements = 5
        dataSource.validationQuery = "select version();"
        dataSource.url = url
        dataSource.autoCommitOnReturn = true
        dataSource.defaultSchema = schema
        val platform = PlatformFactory.createNewPlatformInstance(dataSource)
        platform.createTables(esiDatabase, false, false)
        dataSource.close()
        println("step3UploadDdl: ${TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)}ms")
    }
}
