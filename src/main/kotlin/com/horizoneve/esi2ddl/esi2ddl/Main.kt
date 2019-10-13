package com.horizoneve.esi2ddl.esi2ddl

import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException

/**
 * @author alexm
 */
fun main(args: Array<String>) {
    val parser = DefaultParser()
    val opts = Options()
    opts.addOption("e", "esi-swagger", true, "ESI swagger specification URL (default https://esi.evetech.net/latest/swagger.json?datasource=tranquility)")
    opts.addOption("d", "dry-run", false, "Parses swagger but skips db step3UploadDdl")
    opts.addOption("u", "username", true, "Postgres database username (default postgres)")
    opts.addOption("p", "password", true, "Postgres database password")
    opts.addOption("c", "connection-url", true, "Postgres database connection url string")
    opts.addOption("s", "schema", true, "Postgres database schema (default public)")
    opts.addOption("m", "mapping", true, "Path to ESI API To Database tables mapping information file")
    opts.addOption("v", "verbose", false, "Produces additional output lines")
    opts.addOption("h", "help", false, "Print command line argument help and usage")
    lateinit var esi2ddl : Esi2ddl
    try {
        val line = parser.parse(opts, args)
        if (line.hasOption("help")) {
            printUsage(opts)
            System.exit(0)
        }
        var swagger = line.getOptionValue("esi-swagger")
        if (swagger == null) {
            swagger = "https://esi.evetech.net/latest/swagger.json?datasource=tranquility"
        }
        val url = line.getOptionValue("connection-url")
        if (url == null) {
            argError( "connection-url is required argument", opts)
        }
        val dry = line.hasOption("dry-run")
        var username = line.getOptionValue("username")
        if (username == null)
            username = "postgres"
        val password = line.getOptionValue("password")
        var schema = line.getOptionValue("schema")
        if (schema == null)
            schema = "public"
        var verbose = line.hasOption("verbose")
        var mapping : String? = line.getOptionValue("mapping")

        esi2ddl = Esi2ddl(swagger, url, dry, username, password, schema, mapping, verbose)
    }
    catch(e: ParseException) {
        argError("Error parsing command line arguments: " + e.message, opts)
    }
    esi2ddl.run()
}

fun argError(msg: String, opts: Options) {
    println(msg)
    printUsage(opts)
    System.exit(-1)
}

fun printUsage(opts: Options) {
    val fmt = HelpFormatter()
    fmt.printHelp(120, "[-d] [-v] [-h] -c <connection-url> [-e <esi-swagger>] [-s <schema>] [-u <username>] [-p <password>] [-m <mapping>]",
        "Generates Postgres tables from ESI swagger specification", opts, System.lineSeparator(), false)
}
