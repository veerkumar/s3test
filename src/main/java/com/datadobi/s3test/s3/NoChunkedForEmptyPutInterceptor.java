/*
 *
 *  Copyright Datadobi
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software

 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.datadobi.s3test.s3;

import software.amazon.awssdk.core.SelectedAuthScheme;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkInternalExecutionAttribute;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4FamilyHttpSigner;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.identity.spi.Identity;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class NoChunkedForEmptyPutInterceptor implements ExecutionInterceptor {
    @Override
    public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
        if (context.request() instanceof PutObjectRequest put) {
            Long contentLength = put.contentLength();
            if (contentLength != null && contentLength == 0L) {
                SelectedAuthScheme<?> authScheme = executionAttributes.getAttribute(SdkInternalExecutionAttribute.SELECTED_AUTH_SCHEME);
                if (authScheme != null) {
                    AuthSchemeOption authSchemeOption = authScheme.authSchemeOption();
                    Boolean chunkedEnabled = authSchemeOption.signerProperty(AwsV4FamilyHttpSigner.CHUNK_ENCODING_ENABLED);
                    if (Boolean.TRUE.equals(chunkedEnabled)) {
                        executionAttributes.putAttribute(
                                SdkInternalExecutionAttribute.SELECTED_AUTH_SCHEME,
                                disableChunkedEncoding(authScheme)
                        );
                    }
                }
            }
        }
    }

    private static <T extends Identity> SelectedAuthScheme<T> disableChunkedEncoding(SelectedAuthScheme<T> authScheme) {
        return new SelectedAuthScheme<T>(
                authScheme.identity(),
                authScheme.signer(),
                authScheme.authSchemeOption()
                        .toBuilder()
                        .putSignerProperty(
                                AwsV4FamilyHttpSigner.CHUNK_ENCODING_ENABLED,
                                Boolean.FALSE
                        )
                        .build()
        );
    }
}
