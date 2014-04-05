/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;

/**
 * The BasicOperationProcessor belongs to the BasicOperationService and is responsible for scheduling
 * operations/packets to the correct threads.
 * <p/>
 * The actual processing of the 'task' that is scheduled, is forwarded to the {@link BasicOperationProcessor}. So
 * this class is purely responsible for assigning a 'task' to a particular thread.
 * <p/>
 * The {@link #execute(Object, int, boolean)} accepts an Object instead of a runnable to prevent needing to
 * create wrapper runnables around tasks. This is done to reduce the amount of object litter and therefor
 * reduce pressure on the gc.
 * <p/>
 * There are 2 category of operation threads:
 * <ol>
 * <li>partition specific operation threads: these threads are responsible for executing e.g. a map.put.
 * Operations for the same partition, always end up in the same thread.
 * </li>
 * <li>
 * generic operation threads: these threads are responsible for executing operations that are not
 * specific to a partition. E.g. a heart beat or a map.size.
 * </li>
 * </ol>
 */
public final class BasicOperationScheduler {

    public static final int TERMINATION_TIMEOUT_SECONDS = 3;

    private final ILogger logger;

    private final Node node;
    private final BasicOperationProcessor processor;

    //all operations for specific partitions will be executed on these threads, .e.g map.put(key,value).
    private final OperationThread[] partitionOperationThreads;

    //all operations that are not specific for a partition will be executed here, e.g heartbeat or map.size
    private final OperationThread[] genericOperationThreads;

    //The genericOperationRandom is used when a generic operation is scheduled, and a generic OperationThread
    //needs to be selected.
    //todo:
    //We could have a look at the ThreadLocalRandom, but it requires java 7. So some kind of reflection
    //could to the trick to use something less painful.
    private final Random genericOperationRandom = new Random();

    //The trigger is used when a priority message is send and offered to the operation-thread priority queue.
    //To wakeup the thread, a priorityTaskTrigger is send to the regular blocking queue to wake up the operation
    //thread.
    private final Runnable priorityTaskTrigger = new Runnable() {
        @Override
        public void run() {
        }

        @Override
        public String toString() {
            return "TriggerTask";
        }
    };

    public BasicOperationScheduler(Node node,
                                   ExecutionService executionService,
                                   BasicOperationProcessor processor) {
        this.logger = node.getLogger(BasicOperationScheduler.class);
        this.node = node;
        this.processor = processor;

        this.genericOperationThreads = new OperationThread[getGenericOperationThreadCount()];
        initOperationThreads(genericOperationThreads, new GenericOperationThreadFactory());

        this.partitionOperationThreads = new OperationThread[getPartitionOperationThreadCount()];
        initOperationThreads(partitionOperationThreads, new PartitionOperationThreadFactory());
    }

    private static void initOperationThreads(OperationThread[] operationThreads, ThreadFactory threadFactory) {
        for (int threadId = 0; threadId < operationThreads.length; threadId++) {
            OperationThread operationThread = (OperationThread) threadFactory.newThread(null);
            operationThreads[threadId] = operationThread;
            operationThread.start();
        }
    }

    private int getGenericOperationThreadCount() {
        int threadCount = node.getGroupProperties().GENERIC_OPERATION_THREAD_COUNT.getInteger();
        if (threadCount <= 0) {
            int coreSize = Runtime.getRuntime().availableProcessors();
            threadCount = coreSize * 2;
        }
        return threadCount;
    }

    private int getPartitionOperationThreadCount() {
        int threadCount = node.getGroupProperties().PARTITION_OPERATION_THREAD_COUNT.getInteger();
        if (threadCount <= 0) {
            int coreSize = Runtime.getRuntime().availableProcessors();
            threadCount = coreSize * 2;
        }
        return threadCount;
    }

    boolean isAllowedToRunInCurrentThread(int partitionId) {
        //todo: do we want to allow non partition specific tasks to be run on a partitionSpecific operation thread?
        if (partitionId < 0) {
            return true;
        }

        Thread currentThread = Thread.currentThread();

        //we are only allowed to execute partition aware actions on an OperationThread.
        if (!(currentThread instanceof OperationThread)) {
            return false;
        }

        OperationThread operationThread = (OperationThread) currentThread;
        //if the operationThread is a not a partition specific operation thread, then we are not allowed to execute
        //partition specific operations on it.
        if (!operationThread.isPartitionSpecific) {
            return false;
        }

        //so it is an partition operation thread, now we need to make sure that this operation thread is allowed
        //to execute operations for this particular partitionId.
        int threadId = operationThread.threadId;
        return toPartitionThreadIndex(partitionId) == threadId;
    }

    boolean isInvocationAllowedFromCurrentThread(int partitionId) {
        Thread currentThread = Thread.currentThread();

        if (currentThread instanceof OperationThread) {
            if (partitionId > -1) {
                //todo: we need to check for isPartitionSpecific
                int threadId = ((OperationThread) currentThread).threadId;
                return toPartitionThreadIndex(partitionId) == threadId;
            }
            return true;
        }

        return true;
    }

    public int getOperationExecutorQueueSize() {
        int size = 0;

        for (OperationThread t : partitionOperationThreads) {
            size += t.workQueue.size();
        }

        for (OperationThread t : genericOperationThreads) {
            size += t.workQueue.size();
        }

        return size;
    }

    public int getPriorityOperationExecutorQueueSize() {
        int size = 0;

        for (OperationThread t : partitionOperationThreads) {
            size += t.priorityQueue.size();
        }

        for (OperationThread t : genericOperationThreads) {
            size += t.priorityQueue.size();
        }

        return size;
    }

    public void execute(final Object task, int partitionId, boolean priority) {
        if (task == null) {
            throw new NullPointerException();
        }

        OperationThread operationThread = getOperationThread(partitionId);
        if (priority) {
            offerWork(operationThread.priorityQueue, task);
            offerWork(operationThread.workQueue, priorityTaskTrigger);
        } else {
            offerWork(operationThread.workQueue, task);
        }
    }

    private OperationThread getOperationThread(int partitionId) {
        if (partitionId < 0) {
            //the task can be executed on a generic operation thread
            int genericThreadIndex = genericOperationRandom.nextInt(genericOperationThreads.length);
            return genericOperationThreads[genericThreadIndex];
        } else {
            //the task needs to be executed on a partition operation thread.
            int partitionThreadIndex = toPartitionThreadIndex(partitionId);
            return partitionOperationThreads[partitionThreadIndex];
        }
    }

    private void offerWork(Queue queue, Object task) {
        //in 3.3 we are going to apply backpressure on overload and then we are going to do something
        //with the return values of the offer methods.
        //Currently the queues are all unbound, so this can't happen anyway.

        boolean offer = queue.offer(task);
        if (!offer) {
            logger.severe("Failed to offer " + task + " to BasicOperationScheduler due to overload");
        }
    }

    private int toPartitionThreadIndex(int partitionId) {
        return partitionId % partitionOperationThreads.length;
    }

    public void shutdown() {
        shutdown(partitionOperationThreads);
        shutdown(genericOperationThreads);
        awaitTermination(partitionOperationThreads);
        awaitTermination(genericOperationThreads);
    }

    private static void shutdown(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            thread.shutdown();
        }
    }

    private static void awaitTermination(OperationThread[] operationThreads) {
        for (OperationThread thread : operationThreads) {
            try {
                thread.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String toString() {
        return "BasicOperationScheduler{"
                + "node=" + node.getThisAddress()
                + '}';
    }

    private class GenericOperationThreadFactory implements ThreadFactory {
        private int threadId;

        @Override
        public OperationThread newThread(Runnable ignore) {
            String threadName = node.getThreadPoolNamePrefix("generic-operation") + threadId;
            OperationThread thread = new OperationThread(threadName, false, threadId);
            thread.setContextClassLoader(node.getConfigClassLoader());
            threadId++;
            return thread;
        }
    }

    private class PartitionOperationThreadFactory implements ThreadFactory {
        private int threadId;

        @Override
        public Thread newThread(Runnable ignore) {
            String threadName = node.getThreadPoolNamePrefix("partition-operation") + threadId;
            OperationThread thread = new OperationThread(threadName, true, threadId);
            thread.setContextClassLoader(node.getConfigClassLoader());
            threadId++;
            return thread;
        }
    }

    public final class OperationThread extends Thread {

        final int threadId;
        final boolean isPartitionSpecific;
        private final BlockingQueue workQueue = new LinkedBlockingQueue();
        private final Queue priorityQueue = new ConcurrentLinkedQueue();
        private volatile boolean shutdown;

        public OperationThread(String name, boolean isPartitionSpecific, int threadId) {
            super(node.threadGroup, name);
            this.isPartitionSpecific = isPartitionSpecific;
            this.threadId = threadId;
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (OutOfMemoryError e) {
                onOutOfMemory(e);
            } catch (Throwable t) {
                logger.severe(t);
            }
        }

        private void doRun() {
            for (;;) {
                Object task;
                try {
                    task = workQueue.take();
                } catch (InterruptedException e) {
                    if (shutdown) {
                        return;
                    }
                    continue;
                }

                if (shutdown) {
                    return;
                }

                processPriorityMessages();
                process(task);
            }
        }

        private void process(Object task) {
            try {
                processor.process(task);
            } catch (Exception e) {
                logger.severe("Failed to process task: " + task + " on partitionThread:" + getName());
            }
        }

        private void processPriorityMessages() {
            for (;;) {
                Object task = priorityQueue.poll();
                if (task == null) {
                    return;
                }

                process(task);
            }
        }

        private void shutdown() {
            shutdown = true;
            workQueue.add(new PoisonPill());
        }

        public void awaitTermination(int timeout, TimeUnit unit) throws InterruptedException {
            join(unit.toMillis(timeout));
        }
    }

    private static class PoisonPill {
        @Override
        public String toString() {
            return "PoisonPill";
        }
    }
}
