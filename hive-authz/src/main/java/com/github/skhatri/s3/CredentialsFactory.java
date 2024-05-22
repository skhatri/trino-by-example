package com.github.skhatri.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CredentialsFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CredentialsFactory.class);

    private CredentialsFactory() {
    }

    static final AWSCredentials create() {
        String accessKey = System.getenv("STORE_KEY");
        String secretKey = System.getenv("STORE_SECRET");
        String identityFile = System.getenv("STORE_TOKEN_FILE");
        String roleArn = System.getenv("STORE_ROLE_ARN");

        AWSCredentials credentials = null;
        if(accessKey != null && secretKey != null) {
            LOGGER.info("using basic AWS credentials {}**\n", accessKey.substring(0, accessKey.length() > 5 ? 5 : accessKey.length()));
            credentials = new BasicAWSCredentials(accessKey, secretKey);
        } else {
            WebIdentityTokenCredentialsProvider.Builder builder = WebIdentityTokenCredentialsProvider.builder();
            if (roleArn != null) {
                LOGGER.info("adding ROLE ARN {}", roleArn);
                builder = builder.roleArn(roleArn);
            }
            if (identityFile != null) {
                LOGGER.info("adding identity file {} to create S3 credentials", identityFile);
                builder = builder.webIdentityTokenFile(identityFile);
            }
            credentials = builder.build().getCredentials();
        }
        return credentials;
    }
}
