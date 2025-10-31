package io.cockroachdb.bigbench.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomData {
    private static final Logger logger = LoggerFactory.getLogger(RandomData.class);

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private static final List<String> firstNames = new ArrayList<>();

    private static final List<String> lastNames = new ArrayList<>();

    private static final List<String> cities = new ArrayList<>();

    private static final List<String> countries = new ArrayList<>();

    private static final List<String> currencies = new ArrayList<>();

    private static final List<String> states = new ArrayList<>();

    private static final List<String> stateCodes = new ArrayList<>();

    private static final List<String> lorem = new ArrayList<>();

    private static List<String> readLines(String path) {
        try (InputStream resource = new ClassPathResource(path).getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(resource))) {
            return reader.lines().collect(Collectors.toList());
        } catch (IOException e) {
            logger.error("", e);
            return Collections.emptyList();
        }
    }

    static {
        firstNames.addAll(readLines("random/firstnames.txt"));
        lastNames.addAll(readLines(("random/surnames.txt")));
        cities.addAll(readLines(("random/cities.txt")));
        states.addAll(readLines(("random/states.txt")));
        stateCodes.addAll(readLines(("random/state_code.txt")));
        lorem.addAll(readLines(("random/lorem.txt")));

        Arrays.stream(Locale.getAvailableLocales())
                .filter(locale -> StringUtils.hasLength(locale.getDisplayCountry(Locale.US)))
                .map(locale -> locale.getDisplayCountry(Locale.US))
                .forEach(countries::add);

        Currency.getAvailableCurrencies().stream()
                .map(Currency::getCurrencyCode)
                .forEach(currencies::add);
    }

    public static Money randomMoney() {
        return Money.of(randomBigDecimal(), randomCurrency());
    }

    public static Money randomMoney(String currency) {
        return Money.of(randomBigDecimal(), currency);
    }

    public static boolean randomBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    public static int randomInt(int start, int end) {
        return ThreadLocalRandom.current().nextInt(start, end);
    }

    public static long randomLong(long start, long end) {
        return ThreadLocalRandom.current().nextLong(start, end);
    }

    public static double randomDouble(double start, double end) {
        return ThreadLocalRandom.current().nextDouble(start, end);
    }

    public static LocalDate randomDate() {
        return LocalDate.now().plusDays(ThreadLocalRandom.current().nextBoolean()
                ? randomInt(0, 90) : -randomInt(0, 90));
    }

    public static LocalTime randomTime() {
        return LocalTime.now().plusHours(ThreadLocalRandom.current().nextBoolean()
                ? randomInt(0, 24) : -randomInt(0, 24));
    }

    public static LocalDateTime randomDateTime() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        return LocalDateTime.now()
                .plusDays(r.nextBoolean() ? randomInt(0, 90) : -randomInt(0, 90))
                .plusHours(r.nextBoolean() ? randomInt(0, 24) : -randomInt(0, 24));
    }

    public static BigDecimal randomBigDecimal() {
        return randomBigDecimal(0, 2 ^ 16);
    }

    public static BigDecimal randomBigDecimal(double origin, double bound) {
        return randomBigDecimal(origin, bound, 2);
    }

    public static BigDecimal randomBigDecimal(double origin, double bound, int scale) {
        return BigDecimal.valueOf(ThreadLocalRandom.current().nextDouble(origin, bound))
                .setScale(scale, RoundingMode.HALF_UP);
    }

    public static <E> E selectRandom(List<E> collection) {
        return collection.get(ThreadLocalRandom.current().nextInt(collection.size()));
    }

    public static <E> E selectRandom(E[] collection) {
        return collection[ThreadLocalRandom.current().nextInt(collection.length)];
    }

    public static <T> T selectRandomWeighted(Collection<T> items, List<Double> weights) {
        if (items.isEmpty()) {
            throw new IllegalArgumentException("Empty collection");
        }
        if (items.size() != weights.size()) {
            throw new IllegalArgumentException("Collection and weights mismatch");
        }

        double totalWeight = weights.stream().mapToDouble(w -> w).sum();
        double randomWeight = ThreadLocalRandom.current().nextDouble() * totalWeight;
        double cumulativeWeight = 0;

        int idx = 0;
        for (T item : items) {
            cumulativeWeight += weights.get(idx++);
            if (cumulativeWeight >= randomWeight) {
                return item;
            }
        }

        throw new IllegalStateException("This is not possible");
    }

    public static String randomFirstName() {
        return selectRandom(firstNames);
    }

    public static String randomLastName() {
        return selectRandom(lastNames);
    }

    public static String randomCity() {
        return StringUtils.capitalize(selectRandom(cities));
    }

    public static String randomPhoneNumber() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder()
                .append("(")
                .append(random.nextInt(9) + 1);
        for (int i = 0; i < 2; i++) {
            sb.append(random.nextInt(10));
        }
        sb.append(") ")
                .append(random.nextInt(9) + 1);
        for (int i = 0; i < 2; i++) {
            sb.append(random.nextInt(10));
        }
        sb.append("-");
        for (int i = 0; i < 4; i++) {
            sb.append(random.nextInt(10));
        }
        return sb.toString();
    }

    public static String randomCountry() {
        return selectRandom(countries);
    }

    public static String randomCurrency() {
        return selectRandom(currencies);
    }

    public static String randomState() {
        return selectRandom(states);
    }

    public static String randomStateCode() {
        return selectRandom(stateCodes);
    }

    public static String randomZipCode() {
        StringBuilder sb = new StringBuilder();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 5; i++) {
            sb.append(random.nextInt(10));
        }
        return sb.toString();
    }

    public static String randomEmail() {
        String sb = randomFirstName().toLowerCase()
                + "."
                + randomLastName().toLowerCase()
                + "@example.com";
        return sb.replace(' ', '.');
    }

    public static String randomLoreIpsum(int min, int max, boolean paragraphs) {
        return new LoreIpsum(min, max, paragraphs).generate();
    }

    public static String randomJson(int rootItems, int nestedItems) {
        ObjectNode root = objectMapper.createObjectNode();
        ArrayNode users = root.putArray("users");

        IntStream.range(0, rootItems).forEach(value -> {
            ObjectNode u = users.addObject();

            u.put("email", randomEmail());
            u.put("firstName", randomFirstName());
            u.put("lastName", randomLastName());
            u.put("telephone", randomPhoneNumber());
            u.put("userName", randomFirstName().toLowerCase());

            ArrayNode addr = u.putArray("addresses");

            IntStream.range(0, nestedItems).forEach(n -> {
                ObjectNode a = addr.addObject();

                a.put("state", randomState());
                a.put("stateCode", randomStateCode());
                a.put("city", randomCity());
                a.put("country", randomCountry());
                a.put("zipCode", randomZipCode());
            });

            users.add(u);
        });

        try {
            return objectMapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final char[] VOWELS = "aeiou".toCharArray();

    private static final char[] CONSONANTS = "bcdfghjklmnpqrstvwxyz".toCharArray();

    public static byte[] randomBytes(int min) {
        byte[] arr = new byte[min];
        ThreadLocalRandom.current().nextBytes(arr);
        return arr;
    }

    public static String toBase64(byte[] arr) {
        Base64.Encoder encoder = Base64.getEncoder();
        return encoder.encodeToString(arr);
    }

    public static byte[] fromBase64(String text) {
        Base64.Decoder decoder = Base64.getDecoder();
        return decoder.decode(text);
    }

    public static String randomString(int min, int max) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(random.nextInt(min, max))
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    public static String randomString(int min) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(min)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    public static String randomWord(int min) {
        StringBuilder sb = new StringBuilder();
        boolean vowelStart = true;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < min; i++) {
            if (vowelStart) {
                sb.append(VOWELS[random.nextInt(VOWELS.length)]);
            } else {
                sb.append(CONSONANTS[random.nextInt(CONSONANTS.length)]);
            }
            vowelStart = !vowelStart;
        }
        return sb.toString();
    }

    public static final List<String> FACTS = Arrays.asList(
            "A cockroach can live for a week without its head. Due to their open circulatory system, and the fact that they breathe through little holes in each of their body segments, "
                    + "they are not dependent on the mouth or head to breathe. The roach only dies because without a mouth, it can't drink water and dies of thirst.",
            "A cockroach can hold its breath for 40 minutes, and can even survive being submerged under water for half an hour. They hold their breath often to help regulate their loss of water.",
            "Cockroaches can run up to three miles in an hour, which means they can spread germs and bacteria throughout a home very quickly.",
            "Newborn German cockroaches become adults in as little as 36 days. In fact, the German cockroach is the most common of the cockroaches and has been implicated in outbreaks of illness and allergic reactions in many people.",
            "A one-day-old baby cockroach, which is about the size of a speck of dust, can run almost as fast as its parents.",
            "The American cockroach has shown a marked attraction to alcoholic beverages, especially beer. They are most likely attracted by the alcohol mixed with hops and sugar.",
            "The world's largest cockroach (which lives in South America) is six inches long with a one-foot wingspan.",
            "Cockroaches are believed to have originated more than 280 million years ago, in the Carboniferous era.",
            "There are more than 4,000 species of cockroaches worldwide, including the most common species, the German cockroach, in addition to other common species, the brownbanded cockroach and American cockroach.",
            "Because they are cold-blooded insects, cockroaches can live without food for one month, but will only survive one week without water.",
            "Cockroaches can eat anything.",
            "Some cockroaches can grow as long as 3 inches.",
            "Roaches can live up to a week without their head.",
            "Cockroaches can survive immense nuclear radiation."
    );

    public static String randomRoachFact() {
        return FACTS.get(ThreadLocalRandom.current().nextInt(FACTS.size()));
    }

    private static class LoreIpsum {
        private final int min;

        private final int max;

        private final boolean paragraphs;

        public LoreIpsum(int min, int max, boolean paragraphs) {
            this.min = min;
            this.max = max;
            this.paragraphs = paragraphs;
        }

        public String generate() {
            return StringUtils.capitalize(paragraphs
                    ? getParagraphs(min, max)
                    : getWords(getCount(min, max), false));
        }

        public String getParagraphs(int min, int max) {
            StringBuilder sb = new StringBuilder();
            ThreadLocalRandom random = ThreadLocalRandom.current();

            for (int j = 0; j < getCount(min, max); j++) {
                for (int i = 0; i < random.nextInt(5) + 2; i++) {
                    sb.append(StringUtils.capitalize(getWords(1, false)))
                            .append(getWords(getCount(2, 20), false))
                            .append(". ");
                }
                sb.append("\n");
            }
            return sb.toString().trim();
        }

        private int getCount(int min, int max) {
            min = Math.max(0, min);
            if (max < min) {
                max = min;
            }
            return max != min ? ThreadLocalRandom.current().nextInt(max - min) + min : min;
        }

        private String getWords(int count, boolean capitalize) {
            StringBuilder sb = new StringBuilder();

            int wordCount = 0;
            ThreadLocalRandom random = ThreadLocalRandom.current();
            while (wordCount < count) {
                String word = lorem.get(random.nextInt(lorem.size()));
                if (capitalize) {
                    if (wordCount == 0 || word.length() > 3) {
                        word = StringUtils.capitalize(word);
                    }
                }
                sb.append(word);
                sb.append(" ");
                wordCount++;
            }
            return sb.toString().trim();
        }
    }

    public static String randomIPv4() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        StringJoiner joiner = new StringJoiner(".");
        joiner.add(r.nextInt(0, 255) + "");
        joiner.add(r.nextInt(0, 255) + "");
        joiner.add(r.nextInt(0, 255) + "");
        joiner.add(r.nextInt(0, 255) + "");
        return joiner.toString();
    }

    private static final char[] hexChars = "0123456789abcdef".toCharArray();

    public static String randomIPv6() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringJoiner joiner = new StringJoiner(":");
        IntStream.range(0, 8).mapToObj(i -> new StringBuilder()).forEach(b -> {
            IntStream.rangeClosed(1, 4).forEach(value ->
                    b.append(hexChars[random.nextInt(0, 16)]));
            joiner.add(b.toString());
        });
        return joiner.toString();
    }
}
