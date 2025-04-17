package com.rbccm.database.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class SchemaLoader {
    private static final Logger logger = LoggerFactory.getLogger(SchemaLoader.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static MessageType loadSchema(String schemaPath) throws IOException {
        logger.info("Loading schema from: {}", schemaPath);
        try (InputStream is = SchemaLoader.class.getClassLoader().getResourceAsStream(schemaPath)) {
            if (is == null) {
                throw new IOException("Schema file not found: " + schemaPath);
            }
            JsonNode schemaJson = mapper.readTree(is);
            return parseSchema(schemaJson);
        }
    }

    private static MessageType parseSchema(JsonNode schemaJson) {
        String name = schemaJson.get("name").asText("record");
        JsonNode fieldsJson = schemaJson.get("fields");
        if (fieldsJson == null || !fieldsJson.isArray()) {
            throw new IllegalArgumentException("Schema must contain a 'fields' array");
        }

        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (JsonNode fieldJson : fieldsJson) {
            String fieldName = fieldJson.get("name").asText();
            String type = fieldJson.get("type").asText();
            String repetition = fieldJson.has("repetition") ? fieldJson.get("repetition").asText() : "OPTIONAL";
            String logicalType = fieldJson.has("logicalType") ? fieldJson.get("logicalType").asText() : null;
            int precision = fieldJson.has("precision") ? fieldJson.get("precision").asInt() : 0;
            int scale = fieldJson.has("scale") ? fieldJson.get("scale").asInt() : 0;

            Type.Repetition rep;
            try {
                rep = Type.Repetition.valueOf(repetition);
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid repetition '{}' for field '{}', defaulting to OPTIONAL", repetition, fieldName);
                rep = Type.Repetition.OPTIONAL;
            }

            Types.PrimitiveBuilder<PrimitiveType> typeBuilder;
            switch (type.toUpperCase()) {
                case "INT32":
                    typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, rep);
                    break;
                case "INT64":
                    typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, rep);
                    break;
                case "BINARY":
                    typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, rep);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }

            if (logicalType != null) {
                switch (logicalType.toUpperCase()) {
                    case "STRING":
                        typeBuilder.as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType());
                        break;
                    case "DATE":
                        typeBuilder.as(org.apache.parquet.schema.LogicalTypeAnnotation.dateType());
                        break;
                    case "TIMESTAMP_MICROS":
                        typeBuilder.as(org.apache.parquet.schema.LogicalTypeAnnotation.timestampType(false, org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS));
                        break;
                    case "DECIMAL":
                        typeBuilder.as(org.apache.parquet.schema.LogicalTypeAnnotation.decimalType(scale, precision));
                        break;
                    default:
                        logger.warn("Unsupported logical type: {} for field: {}", logicalType, fieldName);
                }
            }

            builder.addField(typeBuilder.named(fieldName));
        }

        return builder.named(name);
    }
}