// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.athenz.instanceproviderservice.ca;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.util.io.pem.PemObject;

import java.io.IOException;
import java.io.StringWriter;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;

/**
 * Contains PEM formatted signed certificate
 *
 * @author freva
 */
public class CertificateSerializedPayload {

    @JsonProperty("certificate") @JsonSerialize(using = CertificateSerializer.class)
    public final X509Certificate certificate;

    @JsonCreator
    public CertificateSerializedPayload(@JsonProperty("certificate") X509Certificate certificate) {
        this.certificate = certificate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CertificateSerializedPayload that = (CertificateSerializedPayload) o;

        return certificate.equals(that.certificate);
    }

    @Override
    public int hashCode() {
        return certificate.hashCode();
    }

    @Override
    public String toString() {
        return "CertificateSerializedPayload{" +
                "certificate='" + certificate + '\'' +
                '}';
    }

    public static class CertificateSerializer extends JsonSerializer<X509Certificate> {
        @Override
        public void serialize(
                X509Certificate certificate, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            try (StringWriter stringWriter = new StringWriter(); JcaPEMWriter pemWriter = new JcaPEMWriter(stringWriter)) {
                pemWriter.writeObject(new PemObject("CERTIFICATE", certificate.getEncoded()));
                pemWriter.flush();
                gen.writeString(stringWriter.toString());
            } catch (CertificateEncodingException e) {
                throw new RuntimeException("Failed to encode X509Certificate", e);
            }
        }
    }
}
