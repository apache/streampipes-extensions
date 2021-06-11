package org.apache.streampipes.processors.filters.jvm.processor.cpuburner.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CPUBurner {
    private double load;
    private boolean isRunning;
    private final long rampUpDuration;
    private final long rampUpDelay;
    private final List<BusyThread> threads;

    public CPUBurner(int numCores, double load, long rampUpDuration, long rampUpDelay) {
        this.load = load;
        this.rampUpDuration = rampUpDuration;
        this.rampUpDelay = rampUpDelay;
        this.isRunning = false;
        threads = new ArrayList<>();
        for (int i  = 1; i<=numCores; i++){
            threads.add(new BusyThread("CPUBurner " + i, 0));
        }
    }

    public boolean isRunning(){
        return this.isRunning;
    }

    public void startBurner(double desiredLoad){
        this.load = desiredLoad;
        startBurner();
    }

    public void startBurner(){
        CompletableFuture.runAsync(()->{
            this.threads.forEach(Thread::start);
            double defaultLoad = this.load;
            this.load = 0;
            this.isRunning = true;
            updateLoad(defaultLoad);
        });
    }

    public void updateLoad(double desiredLoad){
        long startTime = System.currentTimeMillis();
        long endTime = startTime + this.rampUpDuration;
        while (System.currentTimeMillis() < endTime){
            double interpolatedLoad = ((System.currentTimeMillis() - startTime)/(double)this.rampUpDuration)*(desiredLoad-this.load) + this.load;
            this.threads.forEach(t -> t.setLoad(interpolatedLoad));
            try {
                Thread.sleep(this.rampUpDelay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.threads.forEach(t -> t.setLoad(desiredLoad));
        this.load = desiredLoad;
    }

    public void stopBurners(){
        this.load = 0;
        this.isRunning = false;
        this.threads.forEach(BusyThread::stopThread);
    }



    private static class BusyThread extends Thread {
        //Modified version of: https://gist.github.com/SriramKeerthi/0f1513a62b3b09fecaeb
        private volatile double load;
        private volatile boolean running;

        /**
         * Constructor which creates the thread
         *
         * @param name     Name of this thread
         * @param load     Load % that this thread should generate
         */
        public BusyThread(String name, double load) {
            super(name);
            this.load = load;
        }

        public void stopThread(){
            //System.out.println("Stopped thread " + this.getName());
            this.running = false;
        }

        public void setLoad(double load){
            //System.out.println("Updated thread " + this.getName() +" with load " + load);
            this.load = load;
        }

        /**
         * Generates the load when run
         */
        @Override
        public void run() {
            //System.out.println("Started thread " + this.getName());
            this.running = true;
            try {
                // Loop until stopped
                while (running) {
                    // Every 100ms, sleep for the percentage of unladen time
                    if (System.currentTimeMillis() % 100 == 0) {
                        Thread.sleep((long) Math.floor((1 - load) * 100));
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
