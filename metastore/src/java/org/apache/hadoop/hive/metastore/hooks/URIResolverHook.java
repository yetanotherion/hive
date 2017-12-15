package org.apache.hadoop.hive.metastore.hooks;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.net.URI;
import java.util.List;

/**
 * Allows different metastore uris to be resolved.
 */
public interface URIResolverHook {

    /**
     * Resolve to a proper thrift uri, or a list of uris, given uri of another scheme.
     * @param uri
     * @return
     */
    public List<URI> resolveURI(URI uri) throws HiveMetaException;
}
