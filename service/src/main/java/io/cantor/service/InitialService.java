package io.cantor.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.cantor.http.Application;
import io.cantor.http.Applications;
import io.cantor.http.Server;
import io.cantor.service.clients.LocalIdGenerator;
import io.cantor.service.clients.Parser;
import io.cantor.service.clients.TimeWatcher;
import io.cantor.service.clients.storage.Storage;
import io.cantor.service.clients.storage.StorageFactory;
import io.cantor.service.rest.IdGenerator;
import io.cantor.service.rest.IdParser;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InitialService {

    private static final String ID_PATTERN = "/id";
    private static final String PARSE_PATTERN = "/info";

    private TimeWatcher watcher;

    private List<Storage> storages;

    public static void main(String[] args) {
        InitialService initialService = new InitialService();
        Runtime.getRuntime().addShutdownHook(new Thread(initialService::destroy));
    }

    public InitialService() {
        Config appConfig = ConfigFactory.load("application");
        String instanceId = Utils.hostname();
        if (null == instanceId || instanceId.equals("localhost") ||
                instanceId.equals("127.0.0.1")) {
            throw new RuntimeException(String.format(
                    "Failed to get hostname; Current hostname is %s. Shutdown service.",
                    instanceId));
        }

        String[] storageList = appConfig.getString("storage.sequence").split(",");
        if (storageList.length <= 0)
            throw new IllegalStateException("Number of storage should be at least 1.");

        storages = new ArrayList<>();
        Map<String, Integer> instancesNumberInStorage = new HashMap<>();
        for (String name : storageList) {
            name = name.trim();
            if (!StorageFactory.supportedStorage().contains(name)) {
                if (log.isWarnEnabled())
                    log.warn("Storage {} is not supported!");

                continue;
            }

            log.info("init storage {}", name);
            Optional<Storage> opt = StorageFactory.getInstance(name, appConfig, instanceId);
            if (!opt.isPresent()) {
                if (log.isErrorEnabled())
                    log.error("create {} Storage failed", name);
                throw new IllegalStateException(String.format("init %s failed", name));
            }
            Storage storage = opt.get();
            int instanceNumber = storage.checkAndRegister(Parser.CURRENT_SCHEMA.maxInstanceCount());
            if (instanceNumber != Storage.ILLEGAL_INSTANCE) {
                storages.add(storage);
                instancesNumberInStorage.put(storage.type(), instanceNumber);
            } else {
                if (log.isWarnEnabled())
                    log.warn("Failed to register instance on {}", storage.type());
            }
        }
        if (storages.isEmpty())
            throw new IllegalStateException("Number of storage should be at least 1.");

        //start time watcher
        watcher = new TimeWatcher(appConfig, storages, instanceId, instancesNumberInStorage);
        watcher.start();

        // init api handlers
        log.info("init api handlers");
        IdGenerator idGenerator = new IdGenerator(storages, watcher);
        Application application = Applications.builder()
                                              .post(ID_PATTERN, idGenerator)
                                              .get(ID_PATTERN, idGenerator)
                                              .get(PARSE_PATTERN, new IdParser())
                                              .build();

        Server server = new Server(application);
        server.startup(8080);
    }

    private void destroy() {
        log.info("stop watcher");
        if (null != watcher)
            watcher.stop();

        // destroy storage
        for (Storage storage : storages) {
            log.info("destroy {} storage", storage.type());
            storage.deregister();
            storage.close();
        }
    }
}
