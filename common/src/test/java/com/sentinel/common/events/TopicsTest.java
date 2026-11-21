package com.sentinel.common.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sentinel.common.kafka.Topics;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** Fails the build if Topics.java and infra/kafka/create-topics.sh list different topics. */
class TopicsTest {

    private static final Pattern CREATE = Pattern.compile("^create_topic\\s+(\\S+)", Pattern.MULTILINE);

    @Test
    void topicsScriptMatchesTopicsClass() throws IOException {
        // Maven runs tests from the module directory (common/), so the script is one level up.
        String script = Files.readString(Path.of("..", "infra", "kafka", "create-topics.sh"));
        Set<String> inScript = new TreeSet<>();
        Matcher m = CREATE.matcher(script);
        while (m.find()) {
            inScript.add(m.group(1));
        }
        assertEquals(new TreeSet<>(Topics.ALL), inScript);
    }
}
