package com.redhat.cajun.navy.mission.cache;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;


public abstract class CacheAccessVerticle extends AbstractVerticle {

    private static final String CACHE_NAME_VARIABLE = "MISSION_CACHE_NAME";
    private static final String CACHE_NAME_DEFAULT = "mission";
    private static final String CRT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt";

    private final Logger logger = LoggerFactory.getLogger(CacheAccessVerticle.class.getName());

    protected RemoteCacheManager client;
    protected RemoteCache<String, String> defaultCache;

    protected abstract void init(Future<Void> startFuture);

    @Override
    public void start(Future<Void> startFuture) {

        String cacheNameTemp = System.getenv(CACHE_NAME_VARIABLE);
        if(cacheNameTemp == null || cacheNameTemp.equals(""))
            cacheNameTemp = CACHE_NAME_DEFAULT;
        final String cacheName = cacheNameTemp;
       
        logger.info("start() cacheName = "+cacheName); 
        vertx.<RemoteCache<String, String>>executeBlocking(fut -> {

        client = new RemoteCacheManager(getConfigBuilder().build());
        RemoteCache<String, String> cache = client.administration().getOrCreateCache(cacheName, "default");
        fut.complete(cache);

        }, res -> {
            if (res.succeeded()) {
                logger.info("Cache connection successfully done to cache: "+cacheName);
                defaultCache = res.result();
                init(startFuture);
            } else {
                logger.fatal("Cache connection error to: "+ cacheName);
                startFuture.fail(res.cause());
            }
        });
    }


    protected ConfigurationBuilder getConfigBuilder() {
        ConfigurationBuilder cfg = null;
        if (System.getenv("KUBERNETES_NAMESPACE") != null) {

            cfg = ClientConfiguration.create(config().getString("jdg.svc.name"),
                    config().getString("jdg.app.name"),
                    config().getString("jdg.app.user.name"),
                    config().getString("jdg.app.user.password"));
            cfg.security().ssl().trustStorePath(CRT_PATH);
        } else { // Incase running local
            cfg = new ConfigurationBuilder().addServer()
                    .host(config().getString("jdg.host", "localhost"))
                    .port(config().getInteger("jdg.port", 11222))
                    .clientIntelligence(ClientIntelligence.BASIC);
        }

        return cfg;

    }

}
