package tech.mlsql.plugins.langserver;

import net.csdn.common.logging.CSLogger;
import net.csdn.common.logging.Loggers;
import org.assertj.core.util.Arrays;
import org.eclipse.lsp4j.*;
import org.eclipse.lsp4j.services.*;
import tech.mlsql.plugins.langserver.launchers.stdio.MLSQLDesktopApp;

import java.util.concurrent.CompletableFuture;

/**
 * 25/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class MLSQLLanguageServer implements LanguageServer, LanguageClientAware {

    private LanguageClient client = null;
    private final TextDocumentService textService;
    private final WorkspaceService workspaceService;
    private CSLogger logger = Loggers.getLogger(MLSQLLanguageServer.class.getName());

    public MLSQLLanguageServer() {
        this.textService = new MLSQLDocumentService();
        this.workspaceService = new MLSQLWorkspaceService();
        Thread server = new Thread(() -> {
            logger.info("start....");
            MLSQLDesktopApp.main(Arrays.array());
        });
        server.setDaemon(true);
        server.start();
    }

    @Override
    public CompletableFuture<InitializeResult> initialize(InitializeParams params) {
        LSContext.parse((params.getInitializationOptions()).toString());
        
        final InitializeResult res = new InitializeResult(new ServerCapabilities());
        ServerCapabilities serverCapabilities = new ServerCapabilities();

        serverCapabilities.setTextDocumentSync(TextDocumentSyncKind.Full);
        final CompletionOptions completionOptions = new CompletionOptions();

        completionOptions.setTriggerCharacters(java.util.Arrays.asList(".", ":", " "));

        completionOptions.setResolveProvider(true);
        serverCapabilities.setCompletionProvider(completionOptions);

        res.setCapabilities(serverCapabilities);

        return CompletableFuture.completedFuture(res);
    }

    @Override
    public CompletableFuture<Object> shutdown() {
        logger.info("shutdown......");
        Thread stopThread = new Thread(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                logger.info("", e);
            }
            exit();
        });
        stopThread.setDaemon(true);
        stopThread.start();
        return CompletableFuture.supplyAsync(Object::new);
    }

    @Override
    public void exit() {
        logger.info("exit......");
        System.exit(0);
    }

    @Override
    public TextDocumentService getTextDocumentService() {
        return this.textService;
    }

    @Override
    public WorkspaceService getWorkspaceService() {
        return this.workspaceService;
    }

    @Override
    public void connect(LanguageClient client) {
        this.client = client;
        return;
    }
}
