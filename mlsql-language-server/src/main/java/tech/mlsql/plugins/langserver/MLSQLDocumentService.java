package tech.mlsql.plugins.langserver;

import net.csdn.common.logging.CSLogger;
import net.csdn.common.logging.Loggers;
import org.eclipse.lsp4j.*;
import org.eclipse.lsp4j.jsonrpc.messages.Either;
import org.eclipse.lsp4j.services.TextDocumentService;
import tech.mlsql.autosuggest.statement.SuggestItem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 25/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class MLSQLDocumentService implements TextDocumentService {

    private CSLogger logger = Loggers.getLogger(MLSQLDocumentService.class.getName());

    private FileTracker fileTracker = new FileTracker();

    @Override
    public void didOpen(DidOpenTextDocumentParams params) {
        fileTracker.openFile(params.getTextDocument().getUri(), params.getTextDocument().getText());
    }

    @Override
    public void didChange(DidChangeTextDocumentParams params) {
        fileTracker.changeFile(params.getTextDocument().getUri(), params.getContentChanges());
    }

    @Override
    public void didClose(DidCloseTextDocumentParams params) {
        fileTracker.closeFile(params.getTextDocument().getUri());
    }

    @Override
    public void didSave(DidSaveTextDocumentParams params) {

    }

    @Override
    public CompletableFuture<Either<List<CompletionItem>, CompletionList>> completion(CompletionParams position) {
        String uri = position.getTextDocument().getUri();
        String sql = fileTracker.getText(uri);

        Map<String, String> params = new HashMap<>();
        params.put("sql", sql);
        params.put("lineNum", (position.getPosition().getLine() + 1) + "");
        params.put("columnNum", position.getPosition().getCharacter() + "");
        params.put("schemaInferUrl", "http://127.0.0.1:9003/run/script");


        AutoSuggestWrapper suggestWrapper = new AutoSuggestWrapper(params);
        List<SuggestItem> suggestItems = suggestWrapper.run();
        List<CompletionItem> items = new java.util.ArrayList();
        for (SuggestItem item : suggestItems) {
            items.add(new CompletionItem(item.name()));
        }
        return CompletableFuture.completedFuture(Either.forLeft(items));
    }

    @Override
    public CompletableFuture<CompletionItem> resolveCompletionItem(CompletionItem unresolved) {
        return CompletableFuture.completedFuture(new CompletionItem());
    }
}
