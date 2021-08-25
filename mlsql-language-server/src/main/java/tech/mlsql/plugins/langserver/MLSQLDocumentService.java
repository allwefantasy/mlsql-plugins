package tech.mlsql.plugins.langserver;

import org.eclipse.lsp4j.DidChangeTextDocumentParams;
import org.eclipse.lsp4j.DidCloseTextDocumentParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.DidSaveTextDocumentParams;
import org.eclipse.lsp4j.services.TextDocumentService;

/**
 * 25/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class MLSQLDocumentService implements TextDocumentService {
    @Override
    public void didOpen(DidOpenTextDocumentParams params) {
        throw new RuntimeException("not implemented yet...");
    }

    @Override
    public void didChange(DidChangeTextDocumentParams params) {
        throw new RuntimeException("not implemented yet...");
    }

    @Override
    public void didClose(DidCloseTextDocumentParams params) {
        throw new RuntimeException("not implemented yet...");
    }

    @Override
    public void didSave(DidSaveTextDocumentParams params) {
        throw new RuntimeException("not implemented yet...");
    }
}
