package tech.mlsql.plugins.langserver;

import org.apache.commons.io.IOUtils;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.TextDocumentContentChangeEvent;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class FileTracker {
    private Map<String, String> sourceByPath = new HashMap<>();

    public FileTracker() {

    }

    public boolean isOpen(String uri) {
        return sourceByPath.containsKey(uri);
    }

    public Set<String> getOpenFiles() {
        return sourceByPath.keySet();
    }

    public void openFile(String path, String text) {
        sourceByPath.put(path, text);
    }

    public String closeFile(String path) {
        return sourceByPath.remove(path);
    }

    public void changeFile(String path, List<TextDocumentContentChangeEvent> contentChanges) {
        for (TextDocumentContentChangeEvent change : contentChanges) {
            if (change.getRange() == null) {
                sourceByPath.put(path, change.getText());
            } else if (sourceByPath.containsKey(path)) {
                String existingText = sourceByPath.get(path);
                String newText = patch(existingText, change);
                sourceByPath.put(path, newText);
            } else {
                System.err.println("Failed to apply changes to code intelligence from path: " + path);
            }
        }
    }

    public Reader getReader(String path) {
        if (path == null) {
            return null;
        }
        Reader reader = null;
        if (sourceByPath.containsKey(path)) {
            //if the file is open, use the edited code
            String code = sourceByPath.get(path);
            reader = new StringReader(code);
        } else {
            File file = new File(getPathFromLanguageServerURI(path).toAbsolutePath().toString());
            if (!file.exists()) {
                return null;
            }
            //if the file is not open, read it from the file system
            try {
                reader = new FileReader(file);
            } catch (FileNotFoundException e) {
                //do nothing
            }
        }
        return reader;
    }

    public String getText(String path) {
        if (sourceByPath.containsKey(path)) {
            return sourceByPath.get(path);
        }
        Reader reader = getReader(path);
        if (reader == null) {
            return null;
        }
        try {
            return IOUtils.toString(reader);
        } catch (IOException e) {
            return null;
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
            }
        }
    }
    
    private String patch(String sourceText, TextDocumentContentChangeEvent change) {
        Range range = change.getRange();
        Position start = range.getStart();
        StringReader reader = new StringReader(sourceText);
        int offset = getOffsetFromPosition(reader, start);
        StringBuilder builder = new StringBuilder();
        builder.append(sourceText.substring(0, offset));
        builder.append(change.getText());
        builder.append(sourceText.substring(offset + change.getRangeLength()));
        return builder.toString();
    }

    public static int getOffsetFromPosition(Reader in, Position position) {
        int targetLine = position.getLine();
        int targetCharacter = position.getCharacter();
        try {
            int offset = 0;
            int line = 0;
            int character = 0;

            while (line < targetLine) {
                int next = in.read();

                if (next < 0) {
                    return offset;
                } else {
                    //don't skip \r here if line endings are \r\n in the file
                    //there may be cases where the file line endings don't match
                    //what the editor ends up rendering. skipping \r will help
                    //that, but it will break other cases.
                    offset++;

                    if (next == '\n') {
                        line++;
                    }
                }
            }

            while (character < targetCharacter) {
                int next = in.read();

                if (next < 0) {
                    return offset;
                } else {
                    offset++;
                    character++;
                }
            }

            return offset;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * Converts an URI from the language server protocol to a Path.
     */
    public static Path getPathFromLanguageServerURI(String apiURI) {
        if (apiURI == null) {
            return null;
        }
        URI uri = URI.create(apiURI);
        Optional<Path> optionalPath = getFilePath(uri);
        if (!optionalPath.isPresent()) {
            if (!apiURI.startsWith("vscode-userdata:/")) {
                System.err.println("Could not find URI " + uri);
            }
            return null;
        }
        return optionalPath.get();
    }

    private static Optional<Path> getFilePath(URI uri) {
        if (!uri.getScheme().equals("file")) {
            return Optional.empty();
        } else {
            return Optional.of(Paths.get(uri));
        }
    }
}
