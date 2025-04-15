package com.rbccm.database.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaLoader {
    private static final Logger logger = LoggerFactory.getLogger(SchemaLoader.class);

    public static MessageType loadSchema(String schemaFilePath) throws IOException {
        logger.info("Loading schema from: {}", schemaFilePath);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> schemaMap;
        try (InputStream is = SchemaLoader.class.getClassLoader().getResourceAsStream(schemaFilePath)) {
            if (is == null) {
                logger.error("Schema file not found: {}", schemaFilePath);
                throw new IOException("Schema file not found: " + schemaFilePath);
            }
            schemaMap = mapper.readValue(is, new TypeReference<Map<String, Object>>() {});
        }

        String schemaName = (String) schemaMap.get("name");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fields = (List<Map<String, Object>>) schemaMap.get("fields");

        List<Type> fieldTypes = new ArrayList<>();
        for (Map<String, Object> field : fields) {
            String fieldName = (String) field.get("name");
            String type = (String) field.get("type");
            String repetition = (String) field.get("repetition");
            String logicalType = (String) field.get("logicalType");

            PrimitiveType.Repetition rep = PrimitiveType.Repetition.valueOf(repetition);
            PrimitiveType.PrimitiveTypeName primitiveType = PrimitiveType.PrimitiveTypeName.valueOf(type);

            if (logicalType != null && logicalType.equals("UTF8")) {
                fieldTypes.add(new PrimitiveType(rep, primitiveType, fieldName, OriginalType.UTF8));
            } else {
                fieldTypes.add(new PrimitiveType(rep, primitiveType, fieldName));
            }
        }

        MessageType schema = new MessageType(schemaName, fieldTypes);
        logger.debug("Loaded schema: {}", schema);
        return schema;
    }
}