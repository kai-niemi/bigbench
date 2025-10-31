package io.cockroachdb.bigbench.expression;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.springframework.util.StringUtils;

import io.cockroachdb.bigbench.util.Money;
import io.cockroachdb.bigbench.util.Networking;
import io.cockroachdb.bigbench.util.RandomData;

public final class ExpressionRegistryBuilder {
    private ExpressionRegistryBuilder() {
    }

    public static ExpressionRegistry build(DataSource dataSource) {
        ExpressionRegistry registry = new DefaultExpressionRegistry();

        addIdFunctions(registry);
        addStringFunctions(registry);
        addDateTimeFunctions(registry);
        addMathFunctions(registry);
        addNetworkFunctions(registry);
        addSQLFunctions(registry, dataSource);
        addRandomFunctions(registry);

        return registry;
    }

    public static void addMathFunctions(ExpressionRegistry registry) {
        registry.addVariable("pi", Math.PI);
        registry.addVariable("e", Math.E);

        registry.addFunction(FunctionDef.builder()
                .withCategory("math")
                .withId("pow")
                .withDescription("Returns the value of X raised to the power of Y.")
                .withReturnValue(Double.class)
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    Number arg2 = (Number) args[1];
                    return Math.pow(arg1.doubleValue(), arg2.doubleValue());
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("math")
                .withId("sqrt")
                .withDescription("Returns the square root X.")
                .withReturnValue(Double.class)
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    return Math.sqrt(arg1.doubleValue());
                })
                .build());
    }

    public static void addRandomFunctions(ExpressionRegistry registry) {
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomFirstName")
                .withDescription("Generate a random first name.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomFirstName())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomLastName")
                .withDescription("Generate a random last name.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomLastName())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomFullName")
                .withDescription("Generate a random full name.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomFirstName() + " " + RandomData.randomLastName())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomCity")
                .withDescription("Generate a random city.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomCity())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomCountry")
                .withDescription("Generate a random country.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomCountry())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomEmail")
                .withDescription("Generate a random e-mail.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomEmail())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomPhoneNumber")
                .withDescription("Generate a random phone number.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomPhoneNumber())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomState")
                .withDescription("Generate a random US state.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomState())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomStateCode")
                .withDescription("Generate a random US state code.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomStateCode())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomZipCode")
                .withDescription("Generate a random US zip code.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomZipCode())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomCurrency")
                .withDescription("Generate a random ISO-4217 currency.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomCurrency())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomBigDecimal")
                .withArgs(List.of("(optional) origin: double", "(optional) bound: double", "(optional) scale: double"))
                .withDescription("Generate a random BigDecimal.")
                .withReturnValue(BigDecimal.class)
                .withFunction(args -> {
                    if (args.length == 2) {
                        Number arg1 = (Number) args[0];
                        Number arg2 = (Number) args[1];
                        return RandomData.randomBigDecimal(arg1.doubleValue(), arg2.doubleValue());
                    } else if (args.length == 3) {
                        Number arg1 = (Number) args[0];
                        Number arg2 = (Number) args[1];
                        Number arg3 = (Number) args[2];
                        return RandomData.randomBigDecimal(arg1.doubleValue(), arg2.doubleValue(), arg3.intValue());
                    }
                    return RandomData.randomBigDecimal();
                })
                .build());

        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomMoney")
                .withDescription("Generate a random Money value (decimal_amount currency)")
                .withReturnValue(Money.class)
                .withFunction(args -> {
                    if (args.length > 0) {
                        String arg1 = (String) args[0];
                        return RandomData.randomMoney(arg1);
                    }
                    return RandomData.randomMoney();
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomBoolean")
                .withDescription("Generate a pseudorandom boolean value")
                .withReturnValue(Boolean.class)
                .withFunction(args -> {
                    return RandomData.randomBoolean();
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomInt")
                .withArgs(List.of("origin: int", "bound: int"))
                .withDescription("Generate a pseudorandom int value between a given range (least and upper bound)")
                .withReturnValue(Integer.class)
                .withFunction(args -> {
                    if (args.length == 2) {
                        Number arg1 = (Number) args[0];
                        Number arg2 = (Number) args[1];

                        return RandomData.randomInt(arg1.intValue(), arg2.intValue());
                    }
                    return RandomData.randomInt(0, Integer.MAX_VALUE);
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomLong")
                .withArgs(List.of("origin: long", "bound: long"))
                .withDescription("Generate a pseudorandom long value between a given range (least and upper bound)")
                .withReturnValue(Long.class)
                .withFunction(args -> {
                    if (args.length == 2) {
                        Number arg1 = (Number) args[0];
                        Number arg2 = (Number) args[1];
                        return RandomData.randomLong(arg1.intValue(), arg2.intValue());
                    }
                    return RandomData.randomLong(0, Long.MAX_VALUE);
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomDouble")
                .withArgs(List.of("origin: int", "bound: int"))
                .withDescription("Generate a pseudorandom int value between a given range (least and upper bound)")
                .withReturnValue(Double.class)
                .withFunction(args -> {
                    if (args.length == 2) {
                        Number arg1 = (Number) args[0];
                        Number arg2 = (Number) args[1];
                        return RandomData.randomDouble(arg1.doubleValue(), arg2.doubleValue());
                    }
                    return RandomData.randomDouble(0, Double.MAX_VALUE);
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomString")
                .withArgs(List.of("min: int", "(optional) max: int"))
                .withDescription("Generate a random string.)")
                .withReturnValue(String.class)
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    if (args.length == 2) {
                        Number arg2 = (Number) args[1];
                        return RandomData.randomString(arg1.intValue(), arg2.intValue());
                    }
                    return RandomData.randomString(arg1.intValue());
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomRoachFact")
                .withDescription("Generate a random roach fact.)")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomRoachFact())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomWord")
                .withArgs(List.of("length: int"))
                .withDescription("Generate a random word.)")
                .withReturnValue(String.class)
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    return RandomData.randomWord(arg1.intValue());
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomBytes")
                .withArgs(List.of("length: int"))
                .withDescription("Generate a random byte array.")
                .withReturnValue("byte[]")
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    return RandomData.randomBytes(arg1.intValue());
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomLoreIpsum")
                .withArgs(List.of("origin: min", "bound: max", "paragraphs: bool"))
                .withDescription("Generate a lore ipsum sentence.")
                .withReturnValue(String.class)
                .withFunction(args -> {
                    Number min = (Number) args[0];
                    Number max = (Number) args[1];
                    Boolean paragraphs = (Boolean) args[2];
                    return RandomData.randomLoreIpsum(min.intValue(), max.intValue(), paragraphs);
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("randomJson")
                .withArgs(List.of("items: int", "nesting: int"))
                .withDescription("Generate a random JSON document.")
                .withReturnValue(String.class)
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    Number arg2 = (Number) args[1];
                    return RandomData.randomJson(arg1.intValue(), arg2.intValue());
                })
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("selectRandom")
                .withArgs(List.of("values: object[]"))
                .withDescription("Select a random item from a value collection.")
                .withReturnValue(Object.class)
                .withFunction(RandomData::selectRandom)
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("random")
                .withId("random")
                .withArgs(List.of("values: object"))
                .withDescription("Select a random item from a value collection.")
                .withReturnValue(Object.class)
                .withFunction(args -> RandomData.selectRandom(Arrays.asList(args)))
                .build());
    }

    public static void addIdFunctions(ExpressionRegistry registry) {
        // ID generation functions
        registry.addFunction(FunctionDef.builder()
                .withCategory("identity")
                .withId("randomUUID")
                .withDescription("Generate a random first name.")
                .withReturnValue(UUID.class)
                .withFunction(args -> UUID.randomUUID())
                .build());
    }

    public static void addNetworkFunctions(ExpressionRegistry registry) {
        registry.addFunction(FunctionDef.builder()
                .withCategory("networking")
                .withId("randomIPv4")
                .withDescription("Generate a random IPv4 address.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomIPv4())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("networking")
                .withId("randomIPv6")
                .withDescription("Generate a random IPv6 address.")
                .withReturnValue(String.class)
                .withFunction(args -> RandomData.randomIPv6())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("networking")
                .withId("localIPv4")
                .withDescription("Return local IPv6 address (behind NAT).")
                .withReturnValue(String.class)
                .withFunction(args -> Networking.getLocalIP())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("networking")
                .withId("publicIPv4")
                .withDescription("Return public IPv6 address.")
                .withReturnValue(String.class)
                .withFunction(args -> Networking.getPublicIP())
                .build());
    }

    public static void addDateTimeFunctions(ExpressionRegistry registry) {
        // Time and date functions
        registry.addFunction(FunctionDef.builder()
                .withCategory("temporal")
                .withId("randomDate")
                .withDescription("Generate a random date.")
                .withReturnValue(LocalDate.class)
                .withFunction(args -> RandomData.randomDate())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("temporal")
                .withId("randomTime")
                .withDescription("Generate a random time.")
                .withReturnValue(LocalTime.class)
                .withFunction(args -> RandomData.randomTime())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("temporal")
                .withId("randomDateTime")
                .withDescription("Generate a random date/time name.")
                .withReturnValue(LocalDateTime.class)
                .withFunction(args -> RandomData.randomDateTime())
                .build());

        registry.addFunction(FunctionDef.builder()
                .withCategory("temporal")
                .withId("currentDate")
                .withDescription("Generate current date.")
                .withReturnValue(LocalDate.class)
                .withFunction(args -> LocalDate.now())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("temporal")
                .withId("currentTime")
                .withDescription("Generate current time.")
                .withReturnValue(LocalTime.class)
                .withFunction(args -> LocalTime.now())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("temporal")
                .withId("currentDateTime")
                .withDescription("Generate current date/time name.")
                .withReturnValue(LocalDateTime.class)
                .withFunction(args -> LocalDateTime.now())
                .build());

        registry.addFunction(FunctionDef.builder()
                .withCategory("temporal")
                .withId("plus")
                .withArgs(List.of(
                        "value: java.time.temporal.Temporal",
                        "amountToAdd: long",
                        "unit: java.time.temporal.ChronoUnit"))
                .withDescription("Add a value to a temporal object.")
                .withReturnValue(Temporal.class)
                .withFunction(args -> {
                    Temporal temporal = (Temporal) args[0];
                    Number amount = (Number) args[1];
                    ChronoUnit unit = ChronoUnit.valueOf((String) args[2]);
                    return temporal.plus(amount.longValue(), unit);
                })
                .build());

    }

    public static void addStringFunctions(ExpressionRegistry registry) {
        registry.addFunction(FunctionDef.builder()
                .withCategory("text")
                .withId("lowerCase")
                .withArgs(List.of("str: String"))
                .withDescription("Convert a string to lowercase.")
                .withReturnValue(String.class)
                .withFunction(args ->
                        ((String) args[0]).toLowerCase())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("text")
                .withId("upperCase")
                .withArgs(List.of("str: String"))
                .withDescription("Convert a string to uppercase.")
                .withReturnValue(String.class)
                .withFunction(args ->
                        ((String) args[0]).toUpperCase())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("text")
                .withId("capitalize")
                .withArgs(List.of("str: String"))
                .withDescription("Capitalize a string.")
                .withReturnValue(String.class)
                .withFunction(args ->
                        StringUtils.capitalize((String) args[0]).toLowerCase())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("text")
                .withId("trim")
                .withArgs(List.of("str: String"))
                .withDescription("Trim a string from leading and tailing whitespace.")
                .withReturnValue(String.class)
                .withFunction(args ->
                        ((String) args[0]).toLowerCase().trim())
                .build());
        registry.addFunction(FunctionDef.builder()
                .withCategory("text")
                .withId("base64")
                .withArgs(List.of("arr: byte[]"))
                .withDescription("Encode byte array to base64.")
                .withReturnValue(String.class)
                .withFunction(args -> {
                    byte[] arr = (byte[]) args[0];
                    return RandomData.toBase64(arr);
                })
                .build());
    }

    public static void addSQLFunctions(ExpressionRegistry registry, DataSource dataSource) {
        registry.addFunction(FunctionDef.builder()
                .withCategory("sql")
                .withId("selectOne")
                .withArgs(List.of("query: string", "args: object[]"))
                .withDescription("Execute a read-only SQL query with a single row result.")
                .withReturnValue(Object.class)
                .withFunction(SQLFunctions.selectOne(dataSource))
                .build());

        registry.addFunction(FunctionDef.builder()
                .withCategory("sql")
                .withId("unorderedUniqueRowId")
                .withArgs(List.of("batch_size: int"))
                .withDescription("Returns a unique ID without ordering.")
                .withReturnValue(Long.class)
                .withFunction(SQLFunctions.unorderedUniqueRowId(dataSource))
                .build());

        registry.addFunction(FunctionDef.builder()
                .withCategory("sql")
                .withId("uniqueRowId")
                .withArgs(List.of("batch_size: int"))
                .withDescription("Returns a unique ID with ordering.")
                .withReturnValue(Long.class)
                .withFunction(SQLFunctions.uniqueRowId(dataSource))
                .build());
    }
}
