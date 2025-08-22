package com.rsc;

import java.util.concurrent.atomic.AtomicInteger;

public class SchedulerDemo
{
    public static void main(String[] args) throws Exception
    {
        ChunkedConflatingScheduler scheduler =
                new ChunkedConflatingScheduler(3, 50, 1000000);

        scheduler.start();

        final AtomicInteger counter = new AtomicInteger(0);


        class Counter implements ChunkedConflatingScheduler.Chunkable
        {


            @Override
            public boolean runChunk(long timeSliceMs) throws Exception
            {
                counter.incrementAndGet();
                Thread.sleep(100000000);
                return true;
            }
        }

        for( int i = 0; i < 100000; ++i)
        {
            scheduler.submit("aa","key1"+i, new Counter());
            scheduler.submit("bb","key1"+i, new Counter());
            scheduler.submit("cc","key1"+i, new Counter());
            scheduler.submit("dd","key1"+i, new Counter());
            scheduler.submit("ee","key1"+i, new Counter());
            scheduler.submit("ff","key1"+i, new Counter());
            scheduler.submit("gg","key1"+i, new Counter());
            scheduler.submit("hh","key1"+i, new Counter());
            scheduler.submit("ii","key1"+i, new Counter());
            scheduler.submit("jj","key1"+i, new Counter());
        }


        for(;;)
        {
            if(counter.get()>=999999)
            {
                System.out.println("Counter complete");
                break;
            }

            Thread.sleep(10);
        }
/*

        // Simple chunkable that takes N chunks to finish and logs progress
        class DemoTask implements ChunkedConflatingScheduler.Chunkable
        {
            private final String name;
            private int remainingChunks;

            DemoTask(String name, int chunks)
            {
                this.name = name;
                this.remainingChunks = chunks;
            }

            @Override
            public boolean runChunk(long timeSliceMs)
            {
                System.out.printf("[%d] %s running (remaining=%d)%n",
                        System.currentTimeMillis() % 100_000, name, remainingChunks);
                try
                {
                    Thread.sleep(timeSliceMs / 2); // simulate partial work
                } catch (InterruptedException ignored)
                {
                }
                remainingChunks--;
                return remainingChunks <= 0;
            }

            @Override
            public String toString()
            {
                return name;
            }
        }

        // Producer A: push tasks with same key to test conflation
        for (int i = 1; i <= 3; i++)
        {
            scheduler.submit("A", "key1", new DemoTask("A-key1-v" + i, 2));
        }

        // Producer B: multiple distinct keys
        scheduler.submit("B", "kB1", new DemoTask("B1", 1));
        scheduler.submit("B", "kB2", new DemoTask("B2", 3));

        // Producer C: staged update while in-flight
        scheduler.submit("C", "kc", new DemoTask("C-initial", 2));
        new Thread(() -> {
            try
            {
                Thread.sleep(80); // ensure C-initial is in-flight
            } catch (InterruptedException ignored)
            {
            }
            System.out.println("Staging update for C/kc while in-flight");
            scheduler.submit("C", "kc", new DemoTask("C-staged", 1));
        }).start();

        // Let it run for a while
        Thread.sleep(2000);

        scheduler.shutdownGracefully(2, java.util.concurrent.TimeUnit.SECONDS);
        System.out.println("Demo complete");*/
    }
}
