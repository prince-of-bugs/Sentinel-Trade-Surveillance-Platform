package com.sentinel.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sentinel.common.events.NormalizedTradeEvent;
import com.sentinel.common.events.OrderAction;
import com.sentinel.common.events.Side;
import com.sentinel.common.kafka.Topics;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/**
 * The JSON below is the wire format market-sim must produce: field names match the records in
 * {@code common}, timestamps are ISO-8601 strings, enums are their names.
 */
class TradeEventParserTest {

    static final Instant INGESTED = Instant.parse("2026-11-21T14:30:01Z");

    static final String ORDER_JSON = """
            {"eventId":"evt-1","orderId":"ord-1","accountId":"ACC-1","symbol":"AAPL","side":"BUY",
             "action":"CANCEL","price":190.25,"quantity":100,"occurredAt":"2026-11-21T14:30:00Z"}""";

    static final String EXECUTION_JSON = """
            {"eventId":"evt-2","executionId":"exe-1","orderId":"ord-1","accountId":"ACC-1",
             "counterpartyAccountId":"ACC-2","symbol":"AAPL","side":"SELL","price":190.30,"quantity":50,
             "occurredAt":"2026-11-21T14:30:00.250Z"}""";

    private final TradeEventParser parser = new TradeEventParser(JsonMapper.builder().build());

    @Test
    void parsesOrder() {
        NormalizedTradeEvent e = parser.parse(Topics.ORDERS, ORDER_JSON, INGESTED);

        assertThat(e.kind()).isEqualTo(NormalizedTradeEvent.Kind.ORDER);
        assertThat(e.eventId()).isEqualTo("evt-1");
        assertThat(e.accountId()).isEqualTo("ACC-1");
        assertThat(e.side()).isEqualTo(Side.BUY);
        assertThat(e.action()).isEqualTo(OrderAction.CANCEL);
        assertThat(e.price()).isEqualByComparingTo(new BigDecimal("190.25"));
        assertThat(e.quantity()).isEqualTo(100);
        assertThat(e.occurredAt()).isEqualTo(Instant.parse("2026-11-21T14:30:00Z"));
        assertThat(e.ingestedAt()).isEqualTo(INGESTED);
    }

    @Test
    void parsesExecution() {
        NormalizedTradeEvent e = parser.parse(Topics.EXECUTIONS, EXECUTION_JSON, INGESTED);

        assertThat(e.kind()).isEqualTo(NormalizedTradeEvent.Kind.EXECUTION);
        assertThat(e.counterpartyAccountId()).isEqualTo("ACC-2");
        assertThat(e.action()).isNull();
    }

    @Test
    void ignoresUnknownFields() {
        String withExtra = ORDER_JSON.replace("{", "{\"venue\":\"XNAS\",");
        assertThat(parser.parse(Topics.ORDERS, withExtra, INGESTED).eventId()).isEqualTo("evt-1");
    }

    @Test
    void rejectsMalformedJson() {
        assertThatThrownBy(() -> parser.parse(Topics.ORDERS, "{not json", INGESTED))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageStartingWith("unreadable JSON");
    }

    @Test
    void rejectsMissingFieldAndNamesIt() {
        String noAccount = ORDER_JSON.replace("\"accountId\":\"ACC-1\",", "");
        assertThatThrownBy(() -> parser.parse(Topics.ORDERS, noAccount, INGESTED))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("accountId");
    }

    @Test
    void rejectsNonPositiveQuantity() {
        String zeroQty = ORDER_JSON.replace("\"quantity\":100", "\"quantity\":0");
        assertThatThrownBy(() -> parser.parse(Topics.ORDERS, zeroQty, INGESTED))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("quantity");
    }

    @Test
    void rejectsUnknownEnumValue() {
        String badSide = ORDER_JSON.replace("\"BUY\"", "\"SIDEWAYS\"");
        assertThatThrownBy(() -> parser.parse(Topics.ORDERS, badSide, INGESTED))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void rejectsEmptyPayload() {
        assertThatThrownBy(() -> parser.parse(Topics.ORDERS, null, INGESTED))
                .isInstanceOf(InvalidEventException.class)
                .hasMessage("empty payload");
    }

    @Test
    void rejectsUnexpectedTopic() {
        assertThatThrownBy(() -> parser.parse(Topics.PRICES, ORDER_JSON, INGESTED))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining(Topics.PRICES);
    }
}
