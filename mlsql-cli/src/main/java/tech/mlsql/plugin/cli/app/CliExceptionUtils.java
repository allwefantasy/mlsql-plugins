package tech.mlsql.plugin.cli.app;

/**
 * 25/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class CliExceptionUtils {
    public static CliException createUsageExceptionWithHelp(String errorMsg) {
        CliException launcherException = new CliException();
        launcherException.addMessage("mlsql: " + errorMsg);
        launcherException.addMessage("Run 'mlsql help' for usage.");
        return launcherException;
    }
}
