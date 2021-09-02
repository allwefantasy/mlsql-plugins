package tech.mlsql.plugins.langserver;

import net.csdn.common.logging.CSLogger;
import net.csdn.common.logging.Loggers;
import org.eclipse.lsp4j.*;
import org.eclipse.lsp4j.jsonrpc.messages.Either;
import org.eclipse.lsp4j.services.TextDocumentService;
import tech.mlsql.autosuggest.statement.SuggestItem;
import tech.mlsql.common.utils.base.Joiner;

import java.util.*;
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
        List<String> finalTextList = new ArrayList<>();
        int increment = 0;
        //vscode-notebook-cell
        // #ch0000000
        if (uri.startsWith("vscode-notebook-cell")) {
            List<String> cells = new ArrayList<>();
            String[] temp = uri.split("#ch");
            String prefix = temp[0];
            for (String tempUri : fileTracker.getOpenFiles()) {
                if (tempUri.startsWith(prefix)) {
                    cells.add(tempUri);
                }
            }
            Collections.sort(cells);
            for (String cell : cells) {
                String rawText = fileTracker.getText(cell);
                if (cell.trim().equals(uri.trim())) {
                    finalTextList.add(sql);
                    break;
                }
                increment += rawText.split("\n").length;
                finalTextList.add(rawText);
            }
        } else {
            finalTextList.add(sql);
        }

        Map<String, String> initParams = LSContext.initParams;

        String finalSql = Joiner.on("\n").join(finalTextList);
        Map<String, String> params = new HashMap<>();
        params.put("sql", finalSql);
        params.put("lineNum", (position.getPosition().getLine() + 1 + increment) + "");
        params.put("columnNum", position.getPosition().getCharacter() + "");
        String engineURL = initParams.getOrDefault("engine.url", "http://127.0.0.1:9003");
        if (engineURL.startsWith("/")) {
            engineURL = engineURL.substring(1);
        }
        params.put("schemaInferUrl", engineURL + "/run/script");
        params.put("owner", initParams.getOrDefault("user.owner", "admin"));


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
