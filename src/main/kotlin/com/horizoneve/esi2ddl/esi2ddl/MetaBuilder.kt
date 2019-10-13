package com.horizoneve.esi2ddl.esi2ddl

import com.google.gson.Gson
import org.apache.commons.beanutils.DynaBean
import org.apache.ddlutils.model.Column
import org.apache.ddlutils.model.Database
import org.apache.ddlutils.model.Table
import org.apache.ddlutils.model.TypeMap

class MetaBuilder {
    var data: MutableMap<String, Any?> = LinkedHashMap<String, Any?>()
    var operationId: String? = null

    fun addOperation(operationId: String) {
        this.operationId = operationId
        data[operationId] = LinkedHashMap<String, Any>()
    }

    fun getOperation(): MutableMap<String, Any?> {
        return data[operationId] as MutableMap<String, Any?>
    }

    fun put(key: String, value: Any) {
        data[key] = value
    }

    fun addField(name: String, column: String, isString: Boolean) {
        val f = HashMap<String, Any>()
        f["column"] = column
        f["string"] = isString
        var fields = getOperation()["fields"] as MutableMap<String, Any?>?
        if (fields == null) fields = LinkedHashMap<String, Any?>()
        fields[name] = f
        getOperation()["fields"] = fields
    }

    fun addKey(name: String) {
        var fields = getOperation()["key"] as MutableList<String>?
        if (fields == null) {
            fields = ArrayList<String>()
            getOperation()["key"] = fields
        }
        fields.add(name)
    }

    fun createTable(): Table {
        var table = Table()
        table.name = "swagger_mapping"
        table.description = "ESI Swagger spec To Database tables mapping information"
        val cVersion = Column()
        cVersion.name = "version"
        cVersion.description = "ESI Swagger Api version"
        cVersion.isRequired = true
        cVersion.type = TypeMap.VARCHAR
        cVersion.size = "50"

        val cDescription = Column()
        cDescription.name = "description"
        cDescription.description = "ESI Swagger Api Description"
        cDescription.isRequired = false
        cDescription.type = TypeMap.VARCHAR
        cDescription.size = "255"

        val cMapping = Column()
        cMapping.name = "mapping_spec"
        cMapping.description = "ESI 2 DDL Mapping json"
        cMapping.isRequired = true
        cMapping.type = TypeMap.CLOB
        cMapping.size = "2000000" // 2mb max

        table.addColumn(cVersion)
        table.addColumn(cDescription)
        table.addColumn(cMapping)
        return table
    }

    fun createRow(database: Database): DynaBean {
        val row = database.createDynaBeanFor("\"swagger_mapping\"", true)
        row.set("\"version\"", data["version"])
        row.set("\"description\"", data["description"])
        row.set("\"mapping_spec\"", toJson())
        return row
    }

    fun toJson(): String {
        return Gson().toJson(data)
    }
}
