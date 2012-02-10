package org.dataone.cn.indexer.solrhttp;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
  * User: Porter
 * Date: 8/1/11
 * Time: 12:04 PM
 */
/**Serializes SolrElementAdd command and child documents to HTTP stream
 *
 */

public class OutputStreamHttpEntity implements HttpEntity {

    private Header contentType;

    private SolrElementAdd add = null;
    private String encoding = "UTF-8";

    public OutputStreamHttpEntity(SolrElementAdd add, String encoding) {
        this.add = add;
        this.encoding = encoding;
    }

    public boolean isRepeatable() {
        return false;
    }

    public boolean isChunked() {
        return false;
    }

    /**Content length is unknown always returns -1
     *
     * @return -1
     */
    public long getContentLength() {
        return -1;
    }

    /**Default ContentType is UTF-8
     * @return
     */
    public Header getContentType() {
        return contentType;
    }


    /**Content-Type: text/xml; charset=<ContentType>
     *
     * @return
     */
    public Header getContentEncoding() {
        return new BasicHeader("Content-Type", "text/xml; charset=" + encoding + "");
    }


    public InputStream getContent() throws IOException, IllegalStateException {
        throw new Error("MethodNotImplemented");
    }

    public void writeTo(OutputStream outputStream) throws IOException {
        add.serialize(outputStream, encoding);
        outputStream.flush();
        outputStream.close();
    }

    public boolean isStreaming() {
        return false;
    }

    public void consumeContent() throws IOException {
    }

    public void setContentType(Header contentType) {
        this.contentType = contentType;
    }

}
