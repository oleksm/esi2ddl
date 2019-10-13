package com.horizoneve.esi2ddl.esi2ddl

import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.jayway.jsonpath.*
import org.apache.commons.beanutils.DynaBean
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.ddlutils.PlatformFactory
import org.apache.ddlutils.model.Column
import org.apache.ddlutils.model.Database
import org.apache.ddlutils.model.Table
import org.apache.ddlutils.model.TypeMap
import java.io.File
import java.lang.Exception
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
              val mapping: String?,
              val verbose: Boolean) {

    private lateinit var jsonModels: DocumentContext
    private var metaBuilder: MetaBuilder = MetaBuilder()
    private lateinit var esiDatabase: Database

    fun run () {
        step1ParseSwagger()
        step2BuildEsiTableModel()
        step3PrintMapping()
        step4UploadDdl()
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
        System.err.println("step1ParseSwagger: ${TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)}ms")
    }

    // A creature from hell starts here
    private fun step2BuildEsiTableModel() {
        val start = System.nanoTime()
        esiDatabase = Database()
        esiDatabase.name = jsonModels.read("info.description")
        esiDatabase.version = jsonModels.read("info.version")
        metaBuilder.put("description", esiDatabase.name)
        metaBuilder.put("version", esiDatabase.version)
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
            if (verbose) System.err.println("parsing paths.$path")
            val jT = jsonModels.read<Map<String, String>>("paths.$path.get")
            jT ?: continue // nothing to fetch
            val table = Table()
            // don't ask me why (I would not remember)
            table.name = path2table(path)
            table.description = jT["description"]
            esiDatabase.addTable(table)
            val operationId = jsonModels.read("paths.$path.get.operationId") as String
            metaBuilder.addOperation(operationId)
            metaBuilder.getOperation()["table"] = table.name
            metaBuilder.getOperation()["description"] = table.description

            // path params references
            jsonModels.read<List<String>>("paths.$path.get.parameters..\$ref")?.forEach {pr ->
                val name = pr.replace("#/parameters/", "")
                if (pds.containsKey(name)) {
                    if (table.columns.find{it.name == name} == null) {
                        val column = pds[name]!!.clone() as Column
                        table.addColumn(column)
                        metaBuilder.addField(column.name, column.name, false) // TODO: add isString calculation
                    }
                    metaBuilder.addKey(name)
                }
            }
            // Path parameters objects
            jsonModels.read<List<Map<String, String>>>("paths.$path.get.parameters[?(@.in == 'path')]")?.forEach { jp ->
                val name = jp["name"]
                if (table.columns.find{it.name == name} == null) {
                    convertSingleColumn(name!!, jp, true, table)
                    metaBuilder.addKey(name)
                }
            }
            convertTableStructure("paths.$path.get.responses.200.schema", "", table)
        }

        // Add Mapping information
        esiDatabase.addTable(metaBuilder.createTable())

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
        System.err.println("step2BuildEsiTableModel: ${TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)}ms")
    }

    private fun convertTableStructure(path: String, name: String, table: Table) {
        val jtype = jsonModels.read<String>("$path.type")
        when (jtype) {
            "object" -> {
                if (name == "") metaBuilder.getOperation()["type"] = "structure"
                val required = jsonModels.read<List<String>>("$path.required")?.toHashSet()
                for (jColKV in jsonModels.read<Map<String, Map<String, String>>>("$path.properties")) {
                    val cn = jColKV.key
                    val jCol = jColKV.value
                    // flat out inner objects
                    if (jCol["type"] == "object") {
                        convertTableStructure("$path.properties.$cn", "$name${cn}.", table)
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
                metaBuilder.getOperation()["type"] = "primitive"
                convertSingleColumn("$name${table.name}_id", jsonModels.read<Map<String, String>>(path), false, table)
            }
            else -> throw RuntimeException("Unknown model type: $jtype")
        }
    }

    private fun convertSingleColumn(name: String, jCol: Map<String, Any>, required: Boolean, table: Table, idx: Int = table.columnCount) {
        var columnName = to29CharString(name)
        if (table.columns.find{it.name == columnName} == null) {
            val column = Column()
            column.clone()
            column.type = toDatabaseType(jCol["type"]?.toString(), jCol["format"]?.toString())
            column.size = "4000"
            column.name = columnName
            column.description = jCol["description"]?.toString()
            column.isRequired = required
            column.isPrimaryKey = jCol["uniqueItems"] as Boolean? ?: false
            if (verbose) System.err.println("${table.name}.${column.name}, type: ${column.type}, primary: ${column.isPrimaryKey} required: ${column.isRequired}, description: ${column.description}")
            table.addColumn(idx, column)
            metaBuilder.addField(name, column.name, jCol["type"]?.toString() == "string")
        }
    }

    private fun to29CharString(s: String): String {
        var res = s.replace('.', '_')
        if (res.length <= 29) return res
        val h = Integer.toHexString(res.hashCode())
        val cut = res.length - 29 + h.length
        return res.substring(0, (res.length - cut) / 2) + h + res.substring((res.length + cut) / 2)
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
    private fun path2table(path: String): String {
        var tks = path.trim('/').split('/')
        var res = StringBuffer()
        for (i in tks.indices) {
            var tk = tks[i].replace("{", "").replace("}", "").replace("_id", "")
                .replace("division", "div")
                .replace(Regex("ies$"), "y")
            if (!tk.endsWith("us")) tk = tk.replace(Regex("s$"), "")
            if (tks.size == 1) {
                res.append(tk)
            }
            else if (i == 0) { // first word becomes a prefix
                res.append(
                    tk.replace("alliance", "alli").replace("calendar", "cal").replace(
                        "character",
                        "chr"
                    ).replace("corporation", "crp").replace("dogma", "dgm").replace("fleet", "flt").replace(
                        "incursions",
                        "inc"
                    ).replace("industry", "ind").replace("insurance", "ins").replace("killmail", "km").replace(
                        "loyalty",
                        "loy"
                    ).replace("market", "mkt").replace("opportunity", "opp").replace(
                        "search",
                        "srch"
                    ).replace("sovereignty", "sov").replace("universe", "uv").replace("contract", "ctr")
                ).append('_')
            }
            else if (i == tks.size - 1) {// last word may indicate details
                if (tks[i-1].contains(tk))
                    res.append("dtl")
                else res.append(tk)
            }
            else {
                if (!tks[i-1].contains(tk)) // skip repetitions
                    res.append(tk).append('_')
            }
        }
        if (res.length > 30) throw Exception("table name > 30: ${res.toString()}, tks: $tks")
        return res.toString()
    }

    private fun step3PrintMapping() {
        val start = System.nanoTime()
        if (mapping != null) {
            File(mapping).writeText(metaBuilder.toJson())
        }
        else if (verbose){
            println(metaBuilder.toJson())
        }
        System.err.println("step3PrintMapping: ${TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)}ms")
    }

    private fun step4UploadDdl() {
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
        platform.isScriptModeOn = true
        platform.isSqlCommentsOn = true
        if (dry)
            println(platform.getCreateTablesSql(esiDatabase, false, false))
        else {
            platform.createTables(esiDatabase, false, false)
            platform.insert(esiDatabase, metaBuilder.createRow(esiDatabase))
        }

        dataSource.close()
        System.err.println("step4UploadDdl: ${TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)}ms")
    }
}
