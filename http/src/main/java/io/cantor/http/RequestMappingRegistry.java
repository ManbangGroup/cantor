package io.cantor.http;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RequestMappingRegistry<S extends AffinityScheduler, I, O> {
    PathNode root = new PathNode(PathNode.WILDCARD);
    @Getter
    private final Map<String, String> apiDescriptor = new HashMap<>();

    public void registry(@NonNull HttpMethodKey method, @NonNull String path, AppRequestResponseHandler<S, I, O> handler) {
        apiDescriptor.put(path, method.toString());
        if (path.equals("/") || path.equals("")) {
            root.actions.put(method, handler);
            return;
        }

        String[] segments = split(path);
        PathNode current = root;
        for (String segment : segments) {
            PathNode pathNode = current.next(segment);
            if (pathNode == null) {
                if (isWildcard(segment)) {
                    String wildcardName = segment.substring(1, segment.length() - 1);
                    pathNode = new WildcardNode(wildcardName);
                } else {
                    pathNode = new PathNode(segment);
                }
                current.add(pathNode);
            }

            if (pathNode.getClass().equals(WildcardNode.class) && !isWildcard(segment)) {
                pathNode = new PathNode(segment);
                current.add(pathNode);
            }
            current = pathNode;
        }
        current.actions.put(method, handler);
    }

    public PathMatcher find(HandlerRequest handlerRequest) {
        try {
            HttpMethodKey method;
            try {
                method = HttpMethodKey.valueOf(handlerRequest.method().toUpperCase());
            } catch (IllegalArgumentException illegal) {
                log.debug("method not allowed", illegal);
                return PathMatcher.METHOD_UNMATCHED;
            }
            String path = new URI(handlerRequest.uri()).getPath();

            if (path.equals("/") || path.equals("")) {
                if (root.actions.isEmpty()) {
                    return PathMatcher.RESOURCE_UNMATCHED;
                }
                if (!root.actions.containsKey(method)) {
                    return PathMatcher.METHOD_UNMATCHED;
                }
                return PathMatcher.with(root.actions.get(method));
            }

            String[] segments = split(path);
            PathNode current = root;
            for (String segment : segments) {
                PathNode pathNode = current.next(segment);
                if (pathNode == null) {
                    return PathMatcher.RESOURCE_UNMATCHED;
                }
                if (pathNode.getClass().equals(WildcardNode.class)) {
                    handlerRequest.pathVariables(pathNode.path(), segment);
                }
                current = pathNode;
            }
            if (current.actions.isEmpty()) {
                return PathMatcher.RESOURCE_UNMATCHED;
            }
            if (!current.actions.containsKey(method)) {
                return PathMatcher.METHOD_UNMATCHED;
            }
            return PathMatcher.with(current.actions.get(method));
        } catch (Exception e) {
            return PathMatcher.ERROR;
        }
    }

    private boolean isWildcard(String segment) {
        return segment.matches("^\\{.*\\}$");
    }

    private String[] split(String path) {
        if (path.startsWith("/")) path = path.substring(1);
        if (path.endsWith("/")) path = path.substring(0, path.length() - 1);
        return path.split("/");
    }

    @FunctionalInterface
    interface Matcher {
        boolean match(String segment);
    }

    static class PathNode implements Comparable<PathNode> {
        final static String WILDCARD = "*";
        TreeSet<PathNode> children = new TreeSet<>();
        String path = WILDCARD;
        Map<HttpMethodKey, AppRequestResponseHandler> actions = new HashMap<>();

        PathNode(String path) {
            this.path = path;
        }

        String path() {
            return path;
        }

        Matcher matcher() {
            return (segment) -> path().equals(segment);
        }

        void add(PathNode node) {
            children.add(node);
        }

        PathNode next(String segment) {
            return children.stream()
                           .filter((n) -> n.matcher().match(segment))
                           .findFirst()
                           .orElse(null);
        }

        @Override
        public int compareTo(@NonNull PathNode o) {
            if (getClass().equals(o.getClass())) {
                return this.path.compareTo(o.path);
            } else {
                return -1;
            }
        }
    }

    static class WildcardNode extends PathNode {

        WildcardNode(String placeholder) {
            super(placeholder);
        }

        @Override
        Matcher matcher() {
            return (segment) -> true;
        }

        @Override
        public int compareTo(@NonNull PathNode o) {
            if (getClass().equals(o.getClass())) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    public enum HttpMethodKey {
        GET, POST, PUT, DELETE;
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class PathMatcher {
        @Getter
        private AppRequestResponseHandler serverHandler;
        private int unmatched = 0; // -1 error, 0 matched, 1 resource unmatched, 2 method unmatched
        private final static PathMatcher METHOD_UNMATCHED;
        private final static PathMatcher RESOURCE_UNMATCHED;
        private final static PathMatcher ERROR;
        static {
            PathMatcher resourceUnmatched = new PathMatcher();
            resourceUnmatched.unmatched = 1;
            PathMatcher methodUnmatched = new PathMatcher();
            methodUnmatched.unmatched = 2;
            PathMatcher error = new PathMatcher();
            error.unmatched = -1;

            RESOURCE_UNMATCHED = resourceUnmatched;
            METHOD_UNMATCHED = methodUnmatched;
            ERROR = error;
        }

        private static PathMatcher with(AppRequestResponseHandler serverHandler) {
            if (serverHandler == null) {
                return RESOURCE_UNMATCHED;
            }
            PathMatcher pm = new PathMatcher();
            pm.serverHandler = serverHandler;
            return pm;
        }

        public boolean matched() {
            return unmatched == 0;
        }

        public boolean resourceUnmatched() {
            return unmatched == 1;
        }

        public boolean methodUnmatched() {
            return unmatched == 2;
        }

        public boolean error() {
            return unmatched == -1;
        }
    }
}