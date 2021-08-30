package tech.mlsql.plugins.langserver;

import net.csdn.common.logging.CSLogger;
import net.csdn.common.logging.Loggers;
import org.assertj.core.util.Arrays;
import org.eclipse.lsp4j.InitializeParams;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.ServerCapabilities;
import org.eclipse.lsp4j.TextDocumentSyncKind;
import org.eclipse.lsp4j.jsonrpc.Endpoint;
import org.eclipse.lsp4j.jsonrpc.json.JsonRpcMethod;
import org.eclipse.lsp4j.jsonrpc.json.JsonRpcMethodProvider;
import org.eclipse.lsp4j.jsonrpc.services.ServiceEndpoints;
import org.eclipse.lsp4j.services.*;
import tech.mlsql.plugins.langserver.launchers.stdio.MLSQLDesktopApp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 25/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class MLSQLLanguageServer implements LanguageServer, Endpoint, JsonRpcMethodProvider, LanguageClientAware {

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
        final InitializeResult res = new InitializeResult(new ServerCapabilities());
        res.getCapabilities().setTextDocumentSync(TextDocumentSyncKind.Full);
        return CompletableFuture.supplyAsync(() -> res);
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
    public CompletableFuture<?> request(String s, Object o) {
        System.out.println(s);
        System.out.println(o);
        return CompletableFuture.supplyAsync(Object::new);
    }

    @Override
    public void notify(String s, Object o) {
        System.out.println(s);
        System.out.println(o);
    }

    @Override
    public Map<String, JsonRpcMethod> supportedMethods() {
        Map<String, JsonRpcMethod> supportedMethods = new HashMap<>();
        supportedMethods.putAll(ServiceEndpoints.getSupportedMethods(getClass()));
        return supportedMethods;
    }

    @Override
    public void connect(LanguageClient client) {
        this.client = client;
        return;
    }
}
