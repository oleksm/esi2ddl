package tech.oleks.eveonline.esi.pump

import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.DocumentContext
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option
import java.net.URL
import java.sql.DriverManager
import java.util.concurrent.TimeUnit


class Pump(val swagger: String,
           val username: String,
           val password: String,
           val operationId) {

    lateinit var esi: DocumentContext

    fun run() {
        step1ConnectToDb()
    }

    fun step1ConnectToDb() {
        //Register JDBC driver
        Class.forName("org.postgresql.Driver")

        // Open a connection
        println("Connecting to database...")
        val conn = DriverManager.getConnection(url, username, password)

        // Execute a query
        println("Creating statement...")
        val stmt = conn.createStatement()
        val sql: String
        sql = "SELECT id, first, last, age FROM Employees"
        val rs = stmt.executeQuery(sql)

        //Extract data from result set
        while (rs.next()) {
            //Retrieve by column name
            val id = rs.getInt("id")
            val age = rs.getInt("age")
            val first = rs.getString("first")
            val last = rs.getString("last")

            //Display values
            print("ID: $id")
            print(", Age: $age")
            print(", First: $first")
            println(", Last: $last")
        }
        //Clean-up environment
        rs.close()
        stmt.close()
        conn.close()
    }

    fun step2FetchDatafromEsi() {
        val start = System.nanoTime()
        URL(swagger).openStream()
            .use {
                val conf = Configuration.builder()
                    .options(Option.SUPPRESS_EXCEPTIONS)
                    .build()
                esi = JsonPath.using(conf).parse(it)
            }
        println("step2FetchDatafromEsi: ${TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)}ms")
    }
}
