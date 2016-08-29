package net.sf.jabref.autosave;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.sf.jabref.autosave.event.AutosaveEvent;
import net.sf.jabref.event.BibDatabaseContextChangedEvent;
import net.sf.jabref.model.database.BibDatabaseContext;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class AutoSaver {

    private static Set<AutoSaver> runningInstances = new HashSet<>();

    private final BibDatabaseContext bibDatabaseContext;
    private final BlockingQueue<Runnable> workerQueue;
    private final ExecutorService executor;
    private final EventBus eventBus;


    public AutoSaver(BibDatabaseContext bibDatabaseContext) {
        this.bibDatabaseContext = bibDatabaseContext;
        this.workerQueue = new ArrayBlockingQueue<>(1);
        this.executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, workerQueue);
        this.eventBus = new EventBus();

        System.out.println("I: Initialize AutoSaver...");
        bibDatabaseContext.getDatabase().registerListener(this);
        bibDatabaseContext.getMetaData().registerListener(this);
        runningInstances.add(this);
    }


    @Subscribe
    public synchronized void listen(@SuppressWarnings("unused") BibDatabaseContextChangedEvent event) {
        try {
            executor.submit(() -> {
                eventBus.post(new AutosaveEvent());
            });
        } catch (RejectedExecutionException e) {
            System.out.println("REJECT");
            // do not save while a save process is already running
        }
    }

    private void shutdown() {
        System.out.println("shutting down.");
        bibDatabaseContext.getDatabase().unregisterListener(this);
        bibDatabaseContext.getMetaData().unregisterListener(this);
        executor.shutdown();
    }

    public static void shutdown(BibDatabaseContext bibDatabaseContext) {
        for (AutoSaver autoSaver : runningInstances) {
            if (autoSaver.bibDatabaseContext == bibDatabaseContext) {
                autoSaver.shutdown();
            }
        }
    }

    public void registerListener(Object listener) {
        eventBus.register(listener);
    }

    public void unregisterListener(Object listener) {
        eventBus.unregister(listener);
    }
}
