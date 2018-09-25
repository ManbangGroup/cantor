package io.cantor.service.rest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.cantor.http.AffinityScheduler;
import io.cantor.http.AppRequestResponseHandler;
import io.cantor.http.HandlerRequest;
import io.cantor.http.HandlerResponse;
import io.cantor.service.Utils;
import io.cantor.service.clients.Parser;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IdParser implements
        AppRequestResponseHandler<AffinityScheduler, HandlerRequest, HandlerResponse> {

    private static final String MODE = "mode";
    private static final String ID = "id";
    private static final String NAMESPACE = "np";
    private static final String SEQ = "seq";

    @Override
    public void handle(AffinityScheduler scheduler, HandlerRequest req, HandlerResponse resp) {
        resp.header("Content-Type", "application/json");

        Map<String, String> queries = req.queries();
        if (!queries.containsKey(MODE)) {
            resp.badRequest("Mode parameter is required".getBytes());
            return;
        }
        int mode = Integer.valueOf(queries.get(MODE));

        Set<String> missingQueries = checkMissingParams(queries, mode);
        if (!missingQueries.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("missing queries are: {}", missingQueries);
            resp.badRequest(String.format("Parameters %s are required in mode [%s]",
                    missingQueries.toString(), mode).getBytes());
            return;
        }

        Map<Parser.Info, Number> parsed;
        switch (mode) {
            case Parser.WHOLE_ID:
                parsed = Parser.parseFromNormal(queries.get(ID));
                break;
            case Parser.RADIX:
                parsed = Parser.parserFromRadix(queries.get(ID));
                break;
            default:
                parsed = null;
                break;
        }
        if (null == parsed) {
            if (log.isErrorEnabled())
                log.error("Parsed information of {} is empty", queries.toString());
            resp.internalServerError("parsed result is empty".getBytes());
            return;
        }
        Map<String, Object> data = new HashMap<>();
        if (!Parser.isValid(parsed.get(Parser.Info.CATEGORY).longValue(),
                parsed.get(Parser.Info.TIMESTAMP).longValue(),
                parsed.get(Parser.Info.SEQUENCE).longValue())) {
            resp.badRequest("Illegal ID".getBytes());
            return;
        }
        parsed.forEach((k, v) -> data.put(k.name().toLowerCase(), v));
        resp.ok(Utils.convertToString(data).getBytes());
    }

    private static Set<String> checkMissingParams(Map<String, String> queries, int mode) {

        Set<String> illegalQueries = new HashSet<>();
        if (Parser.WHOLE_ID == mode || Parser.RADIX == mode) {
            if (!queries.containsKey(ID)) {
                illegalQueries.add(ID);
            } else if (null == queries.get(ID) || queries.get(ID).isEmpty()) {
                illegalQueries.add(ID);
            }
            return illegalQueries;
        }

        if (!queries.containsKey(NAMESPACE)) {
            illegalQueries.add(NAMESPACE);
            illegalQueries.add(SEQ);
            return illegalQueries;
        }

        String high = queries.get(NAMESPACE);
        String low = queries.get(SEQ);
        if (null == high || high.isEmpty() || null == low || low.isEmpty()) {
            illegalQueries.add(NAMESPACE);
            illegalQueries.add(SEQ);
        }

        return illegalQueries;
    }
}
