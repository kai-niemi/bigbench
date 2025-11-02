package io.cockroachdb.bigbench.shell;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import io.cockroachdb.bigbench.shell.support.AnsiConsole;
import io.cockroachdb.bigbench.shell.support.ListTableModel;
import io.cockroachdb.bigbench.shell.support.TableUtils;

@ShellComponent
@ShellCommandGroup(CommandGroups.ADMIN_COMMANDS)
public class Admin {
    @Autowired
    @Qualifier("asyncTaskExecutor")
    private ThreadPoolTaskExecutor asyncTaskExecutor;

    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Print local system information", key = {"system-info", "i"})
    public void systemInfo() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        ansiConsole.yellow(">> OS\n");
        ansiConsole.cyan(" Arch: %s | OS: %s | Version: %s\n", os.getArch(), os.getName(), os.getVersion());
        ansiConsole.cyan(" Available processors: %d\n", os.getAvailableProcessors());
        ansiConsole.cyan(" Load avg: %f\n", os.getSystemLoadAverage());

        RuntimeMXBean r = ManagementFactory.getRuntimeMXBean();
        ansiConsole.yellow(">> Runtime\n");
        ansiConsole.cyan(" Uptime: %s\n", r.getUptime());
        ansiConsole.cyan(" VM name: %s | Vendor: %s | Version: %s\n", r.getVmName(), r.getVmVendor(), r.getVmVersion());

        ThreadMXBean t = ManagementFactory.getThreadMXBean();
        ansiConsole.yellow(">> Threads\n");
        ansiConsole.cyan(" Peak threads: %d\n", t.getPeakThreadCount());
        ansiConsole.cyan(" Live thread #: %d\n", t.getThreadCount());
        ansiConsole.cyan(" Total started threads: %d\n", t.getTotalStartedThreadCount());
        ansiConsole.cyan(" Current thread CPU time: %d\n", t.getCurrentThreadCpuTime());
        ansiConsole.cyan(" Current thread User time #: %d\n", t.getCurrentThreadUserTime());

        Arrays.stream(t.getAllThreadIds()).sequential().forEach(value -> {
            ansiConsole.cyan(" Thread (%d): %s %s\n", value,
                    t.getThreadInfo(value).getThreadName(),
                    t.getThreadInfo(value).getThreadState().toString()
            );
        });

        MemoryMXBean m = ManagementFactory.getMemoryMXBean();
        ansiConsole.yellow(">> Memory\n");
        ansiConsole.cyan(" Heap: %s\n", m.getHeapMemoryUsage().toString());
        ansiConsole.cyan(" Non-heap: %s\n", m.getNonHeapMemoryUsage().toString());
        ansiConsole.cyan(" Pending GC: %s\n", m.getObjectPendingFinalizationCount());
    }

    @ShellMethod(value = "Print thread pool metrics", key = {"thread-pool-stats", "tps"})
    public void threadPoolStats() {
        ThreadPoolExecutor tpe = asyncTaskExecutor.getThreadPoolExecutor();

        List<List<?>> tuples = new ArrayList<>();
        tuples.add(List.of("running", asyncTaskExecutor.isRunning()));
        tuples.add(List.of("poolSize", tpe.getPoolSize()));
        tuples.add(List.of("maximumPoolSize", tpe.getMaximumPoolSize()));
        tuples.add(List.of("corePoolSize", tpe.getCorePoolSize()));
        tuples.add(List.of("activeCount", tpe.getActiveCount()));
        tuples.add(List.of("completedTaskCount", tpe.getCompletedTaskCount()));
        tuples.add(List.of("taskCount", tpe.getTaskCount()));
        tuples.add(List.of("largestPoolSize", tpe.getLargestPoolSize()));

        String table = TableUtils.prettyPrint(
                new ListTableModel(tuples, List.of("Property", "Value"))
        );

        ansiConsole.cyan(table).nl();
    }
}
