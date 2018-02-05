// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.prelude.query;

import com.yahoo.prelude.query.textualrepresentation.Discloser;

import java.nio.ByteBuffer;
import java.util.Iterator;


/**
 * A term which contains a fixed length phrase, a collection of word terms,
 * resulting from a single segmentation operation.
 *
 * @author Steinar Knutsen
 */
public class PhraseSegmentItem extends IndexedSegmentItem {

    /** Whether this was explicitly written as a phrase using quotes by the user */
    private boolean explicit = false;

    /**
     * Creates a phrase containing the same words and state (as pertinent) as
     * the given SegmentAndItem.
     */
    public PhraseSegmentItem(AndSegmentItem segAnd) {
        super(segAnd.getRawWord(), segAnd.stringValue(), segAnd.isFromQuery(), segAnd.isStemmed(), segAnd.getOrigin());
        if (segAnd.getItemCount() > 0) {
            WordItem w = (WordItem) segAnd.getItem(0);
            setIndexName(w.getIndexName());
            for (Iterator<Item> i = segAnd.getItemIterator(); i.hasNext();) {
                WordItem word = (WordItem) i.next();
                addWordItem(word);
            }
        }
    }

    public PhraseSegmentItem(String rawWord, boolean isFromQuery, boolean stemmed) {
        super(rawWord, rawWord, isFromQuery, stemmed, null);
    }

    /**
     * Creates a phrase segment from strings
     *
     * @param rawWord the raw text as received in the request
     * @param current the normalized form of the raw text, or the raw text repeated if no normalized form is known
     * @param isFromQuery whether this originates in the request
     * @param stemmed whether this is stemmed
     */
    public PhraseSegmentItem(String rawWord, String current, boolean isFromQuery, boolean stemmed) {
        super(rawWord, current, isFromQuery, stemmed, null);
    }

    public PhraseSegmentItem(String rawWord, String current, boolean isFromQuery,
            boolean stemmed, Substring substring) {
        super(rawWord, current, isFromQuery, stemmed, substring);
    }

    public ItemType getItemType() {
        return ItemType.PHRASE;
    }

    public String getName() {
        return "SPHRASE";
    }

    public void setIndexName(String index) {
        super.setIndexName(index);
        for (Iterator<Item> i = getItemIterator(); i.hasNext();) {
            WordItem word = (WordItem) i.next();
            word.setIndexName(index);
        }
    }

    @Override
    public void setWeight(int weight) {
        super.setWeight(weight);
        for (Iterator<Item> i = getItemIterator(); i.hasNext();) {
            Item word = i.next();
            word.setWeight(weight);
        }
    }

    /**
     * Adds subitem. The word will have its index name set to the index name
     * of this phrase. If the item is a word, it will simply be added,
     * if the item is a phrase, each of the words of the phrase will be added.
     *
     * @throws IllegalArgumentException if the given item is not a WordItem or PhraseItem
     */
    public void addItem(Item item) {
        if (item instanceof WordItem) {
            addWordItem((WordItem) item);
        } else {
            throw new IllegalArgumentException("Can not add " + item + " to a segment phrase");
        }
    }

    private void addWordItem(WordItem word) {
        word.setIndexName(this.getIndexName());
        super.addItem(word);
    }

    // TODO: Override addItem(index,item), setItem(index,item)

    /**
     * Returns a subitem as a word item
     *
     * @param index the (0-base) index of the item to return
     * @throws IndexOutOfBoundsException if there is no subitem at index
     */
    public WordItem getWordItem(int index) {
        return (WordItem) getItem(index);
    }

    protected void encodeThis(ByteBuffer buffer) {
        super.encodeThis(buffer); // takes care of index bytes
    }

    @Override
    public int encode(ByteBuffer buffer) {
        encodeThis(buffer);
        return encodeContent(buffer, 1);
    }

    public int encodeContent(ByteBuffer buffer) {
        return encodeContent(buffer, 0);
    }

    private int encodeContent(ByteBuffer buffer, int itemCount) {
        for (Iterator<Item> i = getItemIterator(); i.hasNext();) {
            Item subitem = i.next();
            itemCount += subitem.encode(buffer);
        }
        return itemCount;
    }


    /**
     * Returns false, no parenthezes for phrases
     */
    protected boolean shouldParenthize() {
        return false;
    }

    /** Segment phrase items uses a empty heading instead of "SPHRASE " */
    protected void appendHeadingString(StringBuilder buffer) {}

    protected void appendBodyString(StringBuilder buffer) {
        appendIndexString(buffer);
        appendContentsString(buffer);
    }

    void appendContentsString(StringBuilder buffer) {
        buffer.append("'");
        for (Iterator<Item> i = getItemIterator(); i.hasNext();) {
            WordItem wordItem = (WordItem) i.next();

            buffer.append(wordItem.getWord());
            if (i.hasNext()) {
                buffer.append(" ");
            }
        }
        buffer.append("'");
    }

    // TODO: Must check all pertinent items
    public boolean equals(Object object) {
        if (!super.equals(object)) {
            return false;
        }
        // PhraseSegmentItem other = (PhraseSegmentItem) object; // Ensured by superclass
        return true;
    }

    public String getIndexedString() {
        StringBuilder buf = new StringBuilder();

        for (Iterator<Item> i = getItemIterator(); i.hasNext();) {
            IndexedItem indexedItem = (IndexedItem) i.next();

            buf.append(indexedItem.getIndexedString());
            if (i.hasNext()) {
                buf.append(' ');
            }
        }
        return buf.toString();
    }

    public boolean isExplicit() {
        return explicit;
    }

    public void setExplicit(boolean explicit) {
        this.explicit = explicit;
    }

    @Override
    public void disclose(Discloser discloser) {
        super.disclose(discloser);
        discloser.addProperty("explicit", explicit);
    }

}
