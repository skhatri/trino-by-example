package com.github.skhatri.s3;

import com.amazonaws.auth.*;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class TrinoCustomAwsCredentialsProvider implements AWSCredentialsProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(TrinoCustomAwsCredentialsProvider.class);

    public TrinoCustomAwsCredentialsProvider(URI uri, Configuration hadoopConf) {
        LOGGER.info("credentials initialised for S3 URI {}", uri);
    }

    @Override
    public AWSCredentials getCredentials() {
        return CredentialsFactory.create();
    }

    @Override
    public void refresh() {

    }


}
