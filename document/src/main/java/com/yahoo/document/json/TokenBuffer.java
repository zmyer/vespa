// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.document.json;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.google.common.base.Preconditions;

/**
 * Helper class to enable lookahead in the token stream.
 *
 * @author <a href="mailto:steinar@yahoo-inc.com">Steinar Knutsen</a>
 */
public class TokenBuffer {
    public static final class Token {
        public final JsonToken token;
        public final String name;
        public final Object text;

        Token(JsonToken token, String name, Object text) {
            this.token = token;
            this.name = name;
            this.text = text;
        }
    }

    private Deque<Token> buffer;
    private int nesting = 0;

    public TokenBuffer() {
        this(new ArrayDeque<>());
    }

    private TokenBuffer(Deque<Token> buffer) {
        this.buffer = buffer;
        if (buffer.size() > 0) {
            updateNesting(buffer.peekFirst().token);
        }
    }

    public JsonToken next() {
        buffer.removeFirst();
        Token t = buffer.peekFirst();
        if (t == null) {
            return null;
        }
        updateNesting(t.token);
        return t.token;
    }

    public JsonToken currentToken() {
        return buffer.peekFirst().token;
    }

    public String currentName() {
        return buffer.peekFirst().name;
    }

    public String currentText() {
        return (String)buffer.peekFirst().text;
    }
    public Number currentNumber() {
        return (Number)buffer.peekFirst().text;
    }
    public Object currentObject() {
        return buffer.peekFirst().text;
    }

    public int size() {
        return buffer.size();
    }

    private void add(JsonToken token, String name, Object text) {
        buffer.addLast(new Token(token, name, text));
    }

    public void bufferObject(JsonToken first, JsonParser tokens) {
        bufferJsonStruct(first, tokens, JsonToken.START_OBJECT);
    }

    private void bufferJsonStruct(JsonToken first, JsonParser tokens, JsonToken firstToken) {
        int localNesting = 0;
        JsonToken t = first;

        Preconditions.checkArgument(first == firstToken,
                "Expected %s, got %s.", firstToken.name(), t);
        if (size() == 0) {
            updateNesting(t);
        }
        localNesting = storeAndPeekNesting(t, localNesting, tokens);
        while (localNesting > 0) {
            t = nextValue(tokens);
            localNesting = storeAndPeekNesting(t, localNesting, tokens);
        }
    }

    private int storeAndPeekNesting(JsonToken t, int nesting, JsonParser tokens) {
        addFromParser(t, tokens);
        return nesting + nestingOffset(t);
    }

    private int nestingOffset(JsonToken t) {
        if (t.isStructStart()) {
            return 1;
        } else if (t.isStructEnd()) {
            return -1;
        } else {
            return 0;
        }
    }

    private void addFromParser(JsonToken t, JsonParser tokens) {
        try {
            switch (t.id()) {
                case JsonTokenId.ID_NUMBER_INT:
                    add(t, tokens.getCurrentName(), tokens.getLongValue());
                    break;
                case JsonTokenId.ID_NUMBER_FLOAT:
                    add(t, tokens.getCurrentName(), tokens.getDoubleValue());
                    break;
                default:
                    add(t, tokens.getCurrentName(), tokens.getText());
                    break;
            }
        } catch (IOException e) {
            // TODO something sane
            throw new RuntimeException(e);
        }
    }

    private JsonToken nextValue(JsonParser tokens) {
        try {
            return tokens.nextValue();
        } catch (IOException e) {
            // TODO something sane
            throw new RuntimeException(e);
        }
    }

    private void updateNesting(JsonToken t) {
        nesting += nestingOffset(t);
    }

    public int nesting() {
        return nesting;
    }

    public Token prefetchScalar(String name) {
        int localNesting = nesting();
        int nestingBarrier = localNesting;
        Token toReturn = null;
        Iterator<Token> i;

        if (name.equals(currentName()) && currentToken().isScalarValue()) {
            toReturn = buffer.peekFirst();
        } else {
            i = buffer.iterator();
            i.next(); // just ignore the first value, as we know it's not what
                      // we're looking for, and it's nesting effect is already
                      // included
            while (i.hasNext()) {
                Token t = i.next();
                if (localNesting == nestingBarrier && name.equals(t.name) && t.token.isScalarValue()) {
                    toReturn = t;
                    break;
                }
                localNesting += nestingOffset(t.token);
                if (localNesting < nestingBarrier) {
                    break;
                }
            }
        }
        return toReturn;
    }
}
