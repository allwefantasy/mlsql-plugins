package tech.mlsql.plugin.cli.app;

import picocli.CommandLine;
import tech.mlsql.core.version.MLSQLVersion;
import tech.mlsql.core.version.VersionInfo;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 25/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class MLSQLCli {

    private static PrintStream errStream = System.err;
    private static PrintStream outStream = System.out;

    public static void main(String[] args) {

    }

    @CommandLine.Command(description = "Default Command.", name = "default")
    private static class DefaultCmd implements MLSQLCmd {

        @CommandLine.Option(names = {"--help", "-h", "?"}, hidden = true, description = "for more information")
        private boolean helpFlag;

        @CommandLine.Option(names = {"--version", "-v"}, hidden = true)
        private boolean versionFlag;

        @CommandLine.Parameters(arity = "0..1")
        private List<String> argList = new ArrayList<>();

        @Override
        public void execute() {
            if (versionFlag) {
                printVersionInfo();
                return;
            }

            if (!argList.isEmpty()) {
                printUsageInfo(argList.get(0));
                return;
            }

            printUsageInfo(CliCommands.HELP);
        }

        @Override
        public String getName() {
            return "default";
        }

        @Override
        public void printLongDesc(StringBuilder out) {

        }

        @Override
        public void printUsage(StringBuilder out) {

        }

        @Override
        public void setParentCmdParser(CommandLine parentCmdParser) {
        }
    }

    private static void printUsageInfo(String commandName) {
        String usageInfo = MLSQLCmd.getCommandUsageInfo(commandName);
        errStream.println(usageInfo);
    }

    private static void printVersionInfo() {
        VersionInfo verison = MLSQLVersion.version();
        String output = "MLSQL: " + verison.version() + "; Spark Core: None";
        outStream.print(output);
    }
}


