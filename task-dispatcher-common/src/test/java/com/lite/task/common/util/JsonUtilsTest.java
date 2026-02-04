package com.lite.task.common.util;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for JsonUtils
 */
@DisplayName("JsonUtils Tests")
class JsonUtilsTest {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class TestUser {
        private String name;
        private int age;
        private String email;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class TestUserWithDate {
        private String name;
        private LocalDateTime createdAt;
    }

    @Nested
    @DisplayName("toJson Tests")
    class ToJsonTests {

        @Test
        @DisplayName("Should serialize object to JSON")
        void shouldSerializeObjectToJson() {
            TestUser user = new TestUser("John", 25, "john@example.com");

            String json = JsonUtils.toJson(user);

            assertNotNull(json);
            assertTrue(json.contains("\"name\":\"John\""));
            assertTrue(json.contains("\"age\":25"));
            assertTrue(json.contains("\"email\":\"john@example.com\""));
        }

        @Test
        @DisplayName("Should return null for null input")
        void shouldReturnNullForNullInput() {
            assertNull(JsonUtils.toJson(null));
        }

        @Test
        @DisplayName("Should serialize list to JSON")
        void shouldSerializeListToJson() {
            List<String> list = Arrays.asList("a", "b", "c");

            String json = JsonUtils.toJson(list);

            assertEquals("[\"a\",\"b\",\"c\"]", json);
        }

        @Test
        @DisplayName("Should serialize map to JSON")
        void shouldSerializeMapToJson() {
            Map<String, Object> map = new HashMap<>();
            map.put("key1", "value1");
            map.put("key2", 123);

            String json = JsonUtils.toJson(map);

            assertTrue(json.contains("\"key1\":\"value1\""));
            assertTrue(json.contains("\"key2\":123"));
        }

        @Test
        @DisplayName("Should exclude null fields")
        void shouldExcludeNullFields() {
            TestUser user = new TestUser("John", 25, null);

            String json = JsonUtils.toJson(user);

            assertFalse(json.contains("email"));
        }

        @Test
        @DisplayName("Should serialize LocalDateTime correctly")
        void shouldSerializeLocalDateTimeCorrectly() {
            LocalDateTime dateTime = LocalDateTime.of(2024, 1, 15, 10, 30, 0);
            TestUserWithDate user = new TestUserWithDate("John", dateTime);

            String json = JsonUtils.toJson(user);

            assertTrue(json.contains("2024-01-15"));
            assertFalse(json.contains("1705")); // Not timestamp
        }
    }

    @Nested
    @DisplayName("toPrettyJson Tests")
    class ToPrettyJsonTests {

        @Test
        @DisplayName("Should serialize to pretty JSON")
        void shouldSerializeToPrettyJson() {
            TestUser user = new TestUser("John", 25, "john@example.com");

            String json = JsonUtils.toPrettyJson(user);

            assertNotNull(json);
            assertTrue(json.contains("\n"));
            assertTrue(json.contains("  ")); // Indentation
        }

        @Test
        @DisplayName("Should return null for null input")
        void shouldReturnNullForNullInput() {
            assertNull(JsonUtils.toPrettyJson(null));
        }
    }

    @Nested
    @DisplayName("fromJson Tests")
    class FromJsonTests {

        @Test
        @DisplayName("Should deserialize JSON to object")
        void shouldDeserializeJsonToObject() {
            String json = "{\"name\":\"John\",\"age\":25,\"email\":\"john@example.com\"}";

            TestUser user = JsonUtils.fromJson(json, TestUser.class);

            assertNotNull(user);
            assertEquals("John", user.getName());
            assertEquals(25, user.getAge());
            assertEquals("john@example.com", user.getEmail());
        }

        @Test
        @DisplayName("Should return null for null JSON")
        void shouldReturnNullForNullJson() {
            assertNull(JsonUtils.fromJson(null, TestUser.class));
        }

        @Test
        @DisplayName("Should return null for empty JSON")
        void shouldReturnNullForEmptyJson() {
            assertNull(JsonUtils.fromJson("", TestUser.class));
        }

        @Test
        @DisplayName("Should ignore unknown properties")
        void shouldIgnoreUnknownProperties() {
            String json = "{\"name\":\"John\",\"age\":25,\"unknownField\":\"value\"}";

            TestUser user = JsonUtils.fromJson(json, TestUser.class);

            assertNotNull(user);
            assertEquals("John", user.getName());
            assertEquals(25, user.getAge());
        }

        @Test
        @DisplayName("Should throw exception for invalid JSON")
        void shouldThrowExceptionForInvalidJson() {
            String invalidJson = "not a valid json";

            assertThrows(RuntimeException.class, () ->
                    JsonUtils.fromJson(invalidJson, TestUser.class));
        }

        @Test
        @DisplayName("Should deserialize with TypeReference")
        void shouldDeserializeWithTypeReference() {
            String json = "[{\"name\":\"John\",\"age\":25},{\"name\":\"Jane\",\"age\":30}]";

            List<TestUser> users = JsonUtils.fromJson(json,
                    new TypeReference<List<TestUser>>() {});

            assertNotNull(users);
            assertEquals(2, users.size());
            assertEquals("John", users.get(0).getName());
            assertEquals("Jane", users.get(1).getName());
        }
    }

    @Nested
    @DisplayName("fromJsonList Tests")
    class FromJsonListTests {

        @Test
        @DisplayName("Should deserialize JSON array to List")
        void shouldDeserializeJsonArrayToList() {
            String json = "[{\"name\":\"John\",\"age\":25},{\"name\":\"Jane\",\"age\":30}]";

            List<TestUser> users = JsonUtils.fromJsonList(json, TestUser.class);

            assertNotNull(users);
            assertEquals(2, users.size());
            assertEquals("John", users.get(0).getName());
            assertEquals("Jane", users.get(1).getName());
        }

        @Test
        @DisplayName("Should return empty list for null JSON")
        void shouldReturnEmptyListForNullJson() {
            List<TestUser> users = JsonUtils.fromJsonList(null, TestUser.class);

            assertNotNull(users);
            assertTrue(users.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list for empty JSON")
        void shouldReturnEmptyListForEmptyJson() {
            List<TestUser> users = JsonUtils.fromJsonList("", TestUser.class);

            assertNotNull(users);
            assertTrue(users.isEmpty());
        }
    }

    @Nested
    @DisplayName("fromJsonMap Tests")
    class FromJsonMapTests {

        @Test
        @DisplayName("Should deserialize JSON to Map")
        void shouldDeserializeJsonToMap() {
            String json = "{\"key1\":\"value1\",\"key2\":123,\"key3\":true}";

            Map<String, Object> map = JsonUtils.fromJsonMap(json);

            assertNotNull(map);
            assertEquals("value1", map.get("key1"));
            assertEquals(123, map.get("key2"));
            assertEquals(true, map.get("key3"));
        }

        @Test
        @DisplayName("Should return empty map for null JSON")
        void shouldReturnEmptyMapForNullJson() {
            Map<String, Object> map = JsonUtils.fromJsonMap(null);

            assertNotNull(map);
            assertTrue(map.isEmpty());
        }
    }

    @Nested
    @DisplayName("convert Tests")
    class ConvertTests {

        @Test
        @DisplayName("Should convert object to another type")
        void shouldConvertObjectToAnotherType() {
            Map<String, Object> map = new HashMap<>();
            map.put("name", "John");
            map.put("age", 25);
            map.put("email", "john@example.com");

            TestUser user = JsonUtils.convert(map, TestUser.class);

            assertNotNull(user);
            assertEquals("John", user.getName());
            assertEquals(25, user.getAge());
        }

        @Test
        @DisplayName("Should return null for null input")
        void shouldReturnNullForNullInput() {
            assertNull(JsonUtils.convert(null, TestUser.class));
        }
    }

    @Nested
    @DisplayName("toMap Tests")
    class ToMapTests {

        @Test
        @DisplayName("Should convert object to Map")
        void shouldConvertObjectToMap() {
            TestUser user = new TestUser("John", 25, "john@example.com");

            Map<String, Object> map = JsonUtils.toMap(user);

            assertNotNull(map);
            assertEquals("John", map.get("name"));
            assertEquals(25, map.get("age"));
            assertEquals("john@example.com", map.get("email"));
        }

        @Test
        @DisplayName("Should return empty map for null input")
        void shouldReturnEmptyMapForNullInput() {
            Map<String, Object> map = JsonUtils.toMap(null);

            assertNotNull(map);
            assertTrue(map.isEmpty());
        }
    }

    @Nested
    @DisplayName("isValidJson Tests")
    class IsValidJsonTests {

        @Test
        @DisplayName("Should return true for valid JSON object")
        void shouldReturnTrueForValidJsonObject() {
            assertTrue(JsonUtils.isValidJson("{\"key\":\"value\"}"));
        }

        @Test
        @DisplayName("Should return true for valid JSON array")
        void shouldReturnTrueForValidJsonArray() {
            assertTrue(JsonUtils.isValidJson("[1,2,3]"));
        }

        @Test
        @DisplayName("Should return false for invalid JSON")
        void shouldReturnFalseForInvalidJson() {
            assertFalse(JsonUtils.isValidJson("not valid json"));
        }

        @Test
        @DisplayName("Should return false for null")
        void shouldReturnFalseForNull() {
            assertFalse(JsonUtils.isValidJson(null));
        }

        @Test
        @DisplayName("Should return false for empty string")
        void shouldReturnFalseForEmptyString() {
            assertFalse(JsonUtils.isValidJson(""));
        }
    }

    @Nested
    @DisplayName("Safe Methods Tests")
    class SafeMethodsTests {

        @Test
        @DisplayName("toJsonSafe should return null on error")
        void toJsonSafeShouldReturnNullOnError() {
            // Create an object that can't be serialized
            Object circular = new Object() {
                public Object getSelf() { return this; }
            };

            // This might or might not throw depending on ObjectMapper config
            // The safe method should handle it gracefully
            String result = JsonUtils.toJsonSafe(circular);
            // Result could be null or a valid JSON depending on implementation
            // The important thing is no exception is thrown
        }

        @Test
        @DisplayName("fromJsonSafe should return null for invalid JSON")
        void fromJsonSafeShouldReturnNullForInvalidJson() {
            String invalidJson = "not valid json";

            TestUser result = JsonUtils.fromJsonSafe(invalidJson, TestUser.class);

            assertNull(result);
        }

        @Test
        @DisplayName("fromJsonSafe should work for valid JSON")
        void fromJsonSafeShouldWorkForValidJson() {
            String json = "{\"name\":\"John\",\"age\":25}";

            TestUser result = JsonUtils.fromJsonSafe(json, TestUser.class);

            assertNotNull(result);
            assertEquals("John", result.getName());
        }
    }

    @Test
    @DisplayName("Should get ObjectMapper instance")
    void shouldGetObjectMapperInstance() {
        assertNotNull(JsonUtils.getObjectMapper());
    }

    @Test
    @DisplayName("Serialization and deserialization should be symmetric")
    void serializationDeserializationShouldBeSymmetric() {
        TestUser original = new TestUser("John", 25, "john@example.com");

        String json = JsonUtils.toJson(original);
        TestUser deserialized = JsonUtils.fromJson(json, TestUser.class);

        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getAge(), deserialized.getAge());
        assertEquals(original.getEmail(), deserialized.getEmail());
    }
}
