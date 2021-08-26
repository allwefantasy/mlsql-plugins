package tech.mlsql.plugin.cli.app;

import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public interface MLSQLCmd {


    void execute();


    String getName();


    void printLongDesc(StringBuilder out);


    void printUsage(StringBuilder out);


    void setParentCmdParser(CommandLine parentCmdParser);


    static String getCommandUsageInfo(String commandName) {
        if (commandName == null) {
            throw CliExceptionUtils.createUsageExceptionWithHelp("invalid command");
        }

        String fileName = "cli-help/mlsql-" + commandName + ".help";
        try {
            return readFileAsString(fileName);
        } catch (IOException e) {
            throw CliExceptionUtils.createUsageExceptionWithHelp("usage info not available for command: " + commandName);
        }
    }

    static String readFileAsString(String path) throws IOException {
        InputStream is = ClassLoader.getSystemResourceAsStream(path);
        InputStreamReader inputStreamREader = null;
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            inputStreamREader = new InputStreamReader(is, StandardCharsets.UTF_8);
            br = new BufferedReader(inputStreamREader);
            String content = br.readLine();
            if (content == null) {
                return sb.toString();
            }

            sb.append(content);

            while ((content = br.readLine()) != null) {
                sb.append('\n').append(content);
            }
        } finally {
            if (inputStreamREader != null) {
                try {
                    inputStreamREader.close();
                } catch (IOException ignore) {
                }
            }
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ignore) {
                }
            }
        }
        return sb.toString();
    }
}