package com.preregistros;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CsvValidador implements Processor {

    private static final String CABECERA_ESPERADA = "patient_id,full_name,appointment_date,insurance_code";
    private static final String CABECERA_ERROR = CABECERA_ESPERADA + ",error_reason";
    private static final Set<String> SEGUROS_VALIDOS = Set.of("IESS", "PRIVADO", "NINGUNO");

    @Override
    public void process(Exchange exchange) {
        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
        String baseName = fileName.replaceAll("\\.csv$", "");
        String[] lineas = exchange.getIn().getBody(String.class).split("\\r?\\n");

        exchange.setProperty("baseName", baseName);

        if (lineas.length == 0 || !lineas[0].trim().equals(CABECERA_ESPERADA)) {
            String motivo = "Cabecera incorrecta: '" + (lineas.length > 0 ? lineas[0].trim() : "") + "'";
            exchange.setProperty("validCsv", "");
            exchange.setProperty("errorCsv", "error_reason\n" + motivo);
            exchange.setProperty("validCount", 0);
            exchange.setProperty("errorCount", 1);
            return;
        }

        StringBuilder validos = new StringBuilder(CABECERA_ESPERADA).append("\n");
        StringBuilder errores = new StringBuilder(CABECERA_ERROR).append("\n");
        int validCount = 0, errorCount = 0;

        for (int i = 1; i < lineas.length; i++) {
            String linea = lineas[i].trim();
            if (linea.isEmpty())
                continue;

            String[] campos = linea.split(",", -1);
            if (campos.length < 4) {
                errores.append(linea).append(",columnas insuficientes\n");
                errorCount++;
                continue;
            }

            List<String> erroresFila = validarCampos(campos);

            if (erroresFila.isEmpty()) {
                validos.append(linea).append("\n");
                validCount++;
            } else {
                errores.append(linea).append(",").append(String.join(" | ", erroresFila)).append("\n");
                errorCount++;
            }
        }

        exchange.setProperty("validCsv", validCount > 0 ? validos.toString().trim() : "");
        exchange.setProperty("errorCsv", errorCount > 0 ? errores.toString().trim() : "");
        exchange.setProperty("validCount", validCount);
        exchange.setProperty("errorCount", errorCount);
    }

    private List<String> validarCampos(String[] campos) {
        List<String> errores = new ArrayList<>();

        String patientId = campos[0].trim();
        String fullName = campos[1].trim();
        String appointDate = campos[2].trim();
        String insuranceCode = campos[3].trim();

        if (patientId.isEmpty() || !patientId.matches("\\d+"))
            errores.add("patient_id inválido: '" + patientId + "'");

        if (fullName.isEmpty())
            errores.add("full_name vacío");

        if (!esFechaValida(appointDate))
            errores.add("appointment_date inválido: '" + appointDate + "' (esperado YYYY-MM-DD)");

        if (!SEGUROS_VALIDOS.contains(insuranceCode.toUpperCase()))
            errores.add("insurance_code inválido: '" + insuranceCode + "' (válidos: IESS, PRIVADO, NINGUNO)");

        return errores;
    }

    private boolean esFechaValida(String valor) {
        if (valor.isEmpty())
            return false;
        try {
            LocalDate.parse(valor);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
