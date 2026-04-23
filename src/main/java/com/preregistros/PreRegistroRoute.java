package com.preregistros;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;

public class PreRegistroRoute extends RouteBuilder {

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        main.configure().addRoutesBuilder(new PreRegistroRoute());
        main.run();
    }

    @Override
    public void configure() {

        // Ruta principal: consume CSVs, valida y distribuye
        from("file:input?delete=true") // delete=true: elimina el archivo de input/ al completar el Exchange, evitando reprocesamiento. El respaldo queda en archive/ vía .to("file:archive")
                .filter(header("CamelFileName").endsWith(".csv"))
                .log("Procesando: ${file:name}")
                .convertBodyTo(String.class)
                .setProperty("csvOriginal", body())
                .setProperty("nombreOriginal", simple("${header.CamelFileName}")) // preservar antes del multicast
                .process(new CsvValidador())
                .multicast().to("direct:guardarValidos", "direct:guardarErrores").end()
                .setBody(simple("${exchangeProperty.csvOriginal}"))                        // restaurar body
                .setHeader("CamelFileName", simple("${exchangeProperty.nombreOriginal}"))  // restaurar nombre
                .to("file:archive")
                .to("direct:registrarLog") // registra resultado en logs/
                .log("Completado: ${exchangeProperty.nombreOriginal} — válidos: ${exchangeProperty.validCount}, inválidos: ${exchangeProperty.errorCount}");

        // Escribe registros válidos en output
        from("direct:guardarValidos")
                .filter(simple("${exchangeProperty.validCsv} != ''"))
                .setBody(simple("${exchangeProperty.validCsv}"))
                .setHeader("CamelFileName", simple("${exchangeProperty.baseName}_validos.csv"))
                .to("file:output");

        // Escribe registros con error en error
        from("direct:guardarErrores")
                .filter(simple("${exchangeProperty.errorCsv} != ''"))
                .setBody(simple("${exchangeProperty.errorCsv}"))
                .setHeader("CamelFileName", simple("${exchangeProperty.baseName}_errores.csv"))
                .to("file:error");

        // Escribe una línea de log por archivo procesado en logs/
        from("direct:registrarLog")
                .transform().simple("${date:now:yyyy-MM-dd HH:mm:ss} | Archivo: ${exchangeProperty.nombreOriginal} | válidos: ${exchangeProperty.validCount} | inválidos: ${exchangeProperty.errorCount}\n")
                .to("file:logs?fileName=preregistros-${date:now:yyyy-MM-dd}.log&fileExist=Append&charset=UTF-8");
    }
}
