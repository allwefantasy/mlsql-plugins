package tech.mlsql.plugins.langserver.launchers.stdio;

import org.eclipse.lsp4j.services.LanguageClient;
import tech.mlsql.plugins.langserver.MLSQLLanguageServer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * 25/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class Launcher {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        System.getProperty("enableOutputStream", "false");
        MLSQLLanguageServer server = new MLSQLLanguageServer();
        org.eclipse.lsp4j.jsonrpc.Launcher<LanguageClient> launcher = org.eclipse.lsp4j.jsonrpc.Launcher.createLauncher(server,
                LanguageClient.class, System.in, System.out);
        LanguageClient client = launcher.getRemoteProxy();
        server.connect(client);
        Future<?> startListening = launcher.startListening();
        startListening.get();

    }
}
