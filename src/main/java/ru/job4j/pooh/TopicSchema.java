package ru.job4j.pooh;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class TopicSchema implements Schema {

    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());
        receivers.get(receiver.name()).add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new LinkedBlockingQueue<>());
        data.get(message.name()).add(message.text());
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            do {
                for (var queueKey : receivers.keySet()) {
                    var queue = data.getOrDefault(queueKey, new LinkedBlockingQueue<>());
                    var receiversByQueue = receivers.get(queueKey);
                    var it = receiversByQueue.iterator();
                    var data = queue.poll();
                    while (it.hasNext()) {
                        if (data != null) {
                            it.next().receive(data);
                        }
                        if (data == null) {
                            break;
                        }
                        if (!it.hasNext()) {
                            it = receiversByQueue.iterator();
                            data = queue.poll();
                        }
                    }
                }
                condition.off();
            } while (condition.check());
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}