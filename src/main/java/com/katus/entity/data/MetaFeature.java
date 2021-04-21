package com.katus.entity.data;

import com.katus.io.reader.Reader;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-12
 * @since 2.0
 */
@Getter
@Setter
public class MetaFeature extends Feature {
    public static String META_ID = "###META###";

    public MetaFeature() {
        super(META_ID, new LinkedHashMap<>());
    }

    public void setReaderHelper(Reader.ReaderHelper helper) {
        super.setAttribute(new Field(META_ID), helper);
    }

    public Reader.ReaderHelper getReaderHelper() {
        return (Reader.ReaderHelper) super.getAttribute(new Field(META_ID));
    }
}
