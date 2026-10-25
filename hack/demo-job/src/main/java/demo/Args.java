package demo;

import java.util.HashMap;
import java.util.Map;

/** Parses --key value arguments. */
final class Args {
    private final Map<String, String> values = new HashMap<>();

    Args(String[] args) {
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("expected --key value, got " + args[i]);
            }
            values.put(args[i].substring(2), args[i + 1]);
        }
    }

    String string(String key, String fallback) {
        return values.getOrDefault(key, fallback);
    }

    long integer(String key, long fallback) {
        String v = values.get(key);
        return v == null ? fallback : Long.parseLong(v);
    }
}
