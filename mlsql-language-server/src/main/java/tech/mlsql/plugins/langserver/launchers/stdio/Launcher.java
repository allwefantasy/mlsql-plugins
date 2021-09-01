package tech.mlsql.plugins.langserver.launchers.stdio;

import org.eclipse.lsp4j.services.LanguageClient;
import tech.mlsql.plugins.langserver.MLSQLLanguageServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;


/**
 * 25/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class Launcher {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        
        MLSQLLanguageServer server = new MLSQLLanguageServer();

        boolean lspInspectorTrace = false;


        org.eclipse.lsp4j.jsonrpc.Launcher<LanguageClient> launcher = null;

        if (lspInspectorTrace) {
            launcher = org.eclipse.lsp4j.jsonrpc.Launcher.createLauncher(server, LanguageClient.class, exitOnClose(System.in), System.out,
                    true, new PrintWriter(System.err));
        } else {
            launcher = org.eclipse.lsp4j.jsonrpc.Launcher.createLauncher(server, LanguageClient.class, System.in, System.out);
        }


        LanguageClient client = launcher.getRemoteProxy();
        server.connect(client);
        launcher.startListening().get();

    }

    private static InputStream exitOnClose(InputStream delegate) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                return exitIfNegative(delegate.read());
            }

            int exitIfNegative(int result) {
                if (result < 0) {
                    System.err.println("Input stream has closed. Exiting...");
                    System.exit(0);
                }
                return result;
            }
        };
    }
}
