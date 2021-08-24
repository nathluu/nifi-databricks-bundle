/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.databricks;

import com.google.common.base.Strings;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.InvokeHTTP;
import org.apache.nifi.processors.standard.util.SoftLimitBoundedByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@CapabilityDescription("An HTTP client processor which will interact with a configurable HTTP Endpoint when azure.filename attribute of incoming flowfile is SUCCESS_YYYY_MM_DD.json . The destination URL and HTTP Method are configurable. FlowFile attributes are converted to HTTP headers and the FlowFile contents are included as the body of the request (if the HTTP Method is PUT, POST or PATCH).")
public class SubmitSparkJobByFileNameProcessor extends InvokeHTTP {
    public static final String AZURE_FILENAME_ATTRIBUTE = "azure.filename";
    public static final String DEFAULT_REGEX_SUCCESS_FILENAME = "^SUCCESS_((19|2[0-9])[0-9]{2})_(0[1-9]|1[012])_(0[1-9]|[12][0-9]|3[01]).json$";
    public static final PropertyDescriptor PROP_BODY;
    public static final PropertyDescriptor PROP_HEADERS;
    public static final PropertyDescriptor PROP_API_TOKEN;
    public static final List<PropertyDescriptor> NEW_PROPERTIES;
    public static final PropertyDescriptor PROP_REGEX_FILENAME;

    static {
        PROP_BODY = (new PropertyDescriptor.Builder()).name("Body")
                .description("Set Body value here")
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                        AttributeExpression.ResultType.STRING))
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
        PROP_HEADERS = (new PropertyDescriptor.Builder()).name("Headers")
                .description("Rows are separated by new line. Keys and values are separated by : ")
                .required(false)
                .addValidator(
                        StandardValidators
                                .createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
        PROP_API_TOKEN = (new PropertyDescriptor.Builder()).name("API Token")
                .description("Regex value to check success file name to submit the rest API")
                .required(false)
                .sensitive(true)
                .addValidator(
                        StandardValidators
                                .createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
        PROP_REGEX_FILENAME = (new PropertyDescriptor.Builder()).name("Success Filename Regex")
                .description("Regex value to check success file name to submit the rest API")
                .defaultValue(DEFAULT_REGEX_SUCCESS_FILENAME)
                .required(true)
                .addValidator(
                        StandardValidators
                                .createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
        NEW_PROPERTIES = Stream.of(PROPERTIES, Arrays.asList(PROP_BODY, PROP_HEADERS, PROP_API_TOKEN, PROP_REGEX_FILENAME)).flatMap(List::stream).collect(
                Collectors.toList());
    }

    private final AtomicReference<OkHttpClient> refNewOkHttpClientAtomicReference;
    private final Method refConfigureRequest;
    private final Method refConvertAttributesFromHeaders;
    private final Method refIsSuccess;
    private final Method refGetCharsetFromMediaType;
    private final Method refRoute;
    private final Method refLogRequest;
    private final Method refLogResponse;

    public SubmitSparkJobByFileNameProcessor() {
        super();

        try {
            Field fieldOkHttpClientAtomicReference = InvokeHTTP.class.getDeclaredField("okHttpClientAtomicReference");
            fieldOkHttpClientAtomicReference.setAccessible(true);
            refNewOkHttpClientAtomicReference = (AtomicReference<OkHttpClient>) fieldOkHttpClientAtomicReference
                    .get(this);

            refConfigureRequest = InvokeHTTP.class.getDeclaredMethod("configureRequest", ProcessContext.class,
                    ProcessSession.class, FlowFile.class, URL.class);
            refConfigureRequest.setAccessible(true);

            refConvertAttributesFromHeaders = InvokeHTTP.class.getDeclaredMethod("convertAttributesFromHeaders",
                    Response.class);
            refConvertAttributesFromHeaders.setAccessible(true);

            refIsSuccess = InvokeHTTP.class.getDeclaredMethod("isSuccess", int.class);
            refIsSuccess.setAccessible(true);

            refGetCharsetFromMediaType = InvokeHTTP.class.getDeclaredMethod("getCharsetFromMediaType",
                    MediaType.class);
            refGetCharsetFromMediaType.setAccessible(true);

            refRoute = InvokeHTTP.class.getDeclaredMethod("route", FlowFile.class, FlowFile.class,
                    ProcessSession.class, ProcessContext.class, int.class);
            refRoute.setAccessible(true);

            refLogRequest = InvokeHTTP.class.getDeclaredMethod("logRequest", ComponentLog.class, Request.class);
            refLogRequest.setAccessible(true);

            refLogResponse = InvokeHTTP.class
                    .getDeclaredMethod("logResponse", ComponentLog.class, URL.class, Response.class);
            refLogResponse.setAccessible(true);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return NEW_PROPERTIES;
    }

    public Request createHttpRequest(ProcessContext context, ProcessSession sesion, FlowFile requestFlowFile,
            URL url) throws Exception {
        try {
            Request httpRequest = (Request) this.refConfigureRequest.invoke(this, context, sesion, requestFlowFile,
                    url);
            String body = context.getProperty(PROP_BODY).evaluateAttributeExpressions(requestFlowFile).getValue();
            String headers = context.getProperty(PROP_HEADERS).evaluateAttributeExpressions(requestFlowFile).getValue();
            String apiToken = context.getProperty(PROP_API_TOKEN).evaluateAttributeExpressions(requestFlowFile).getValue();

            if (Strings.isNullOrEmpty(body)) {
                return httpRequest;
            }

            Request.Builder httpRequestBuilder = httpRequest.newBuilder();

            if (!Strings.isNullOrEmpty(headers)) {
                // Build header
                String[] lines = headers.split("\\r?\\n|\\r");

                for (String line : lines) {
                    String[] arr = line.split(":", 2);

                    // Replace value if exists
                    httpRequestBuilder.header(arr[0], arr[1]);
                }
            }

            if (!Strings.isNullOrEmpty(apiToken)) {
                httpRequestBuilder.header("Authorization", "Bearer " + apiToken);
            }

            httpRequestBuilder.method(httpRequest.method(),
                    RequestBody.create(body, MediaType.get("application/json; charset=utf-8")));

            return httpRequestBuilder.build();
        }
        catch (Exception e) {
            if (e.getCause() instanceof IllegalArgumentException) {
                throw new IllegalArgumentException(e.getCause());
            }

            throw e;
        }
    }

    public boolean shouldTrigger(ProcessContext context, FlowFile requestFlowFile) {
        String filename = requestFlowFile.getAttribute(AZURE_FILENAME_ATTRIBUTE);
        String patternString = context.getProperty(PROP_REGEX_FILENAME).evaluateAttributeExpressions(requestFlowFile)
                .getValue();
        Pattern valid = Pattern.compile(patternString);

        if (valid.matcher(filename).matches()) {
            return true;
        }

        return false;
    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile requestFlowFile = session.get();
        OkHttpClient okHttpClient = (OkHttpClient) this.refNewOkHttpClientAtomicReference.get();
        boolean putToAttribute = context.getProperty(PROP_PUT_OUTPUT_IN_ATTRIBUTE).isSet();

        if (requestFlowFile == null) {
            if (context.hasNonLoopConnection()) {
                return;
            }

            String request = context.getProperty(PROP_METHOD).evaluateAttributeExpressions().getValue().toUpperCase();
            if ("POST".equals(request) || "PUT".equals(request) || "PATCH".equals(request)) {
                return;
            }

            if (putToAttribute) {
                requestFlowFile = session.create();
            }
        }

        if (!shouldTrigger(context, requestFlowFile)) {
            session.remove(requestFlowFile);
            return;
        }

        int maxAttributeSize = context.getProperty(PROP_PUT_ATTRIBUTE_MAX_LENGTH).asInteger();
        ComponentLog logger = this.getLogger();
        UUID txId = UUID.randomUUID();
        FlowFile responseFlowFile = null;

        try {
            String urlProperty = StringUtils.trimToEmpty(
                    context.getProperty(PROP_URL).evaluateAttributeExpressions(requestFlowFile).getValue());
            URL url = new URL(urlProperty);
            Request httpRequest = createHttpRequest(context, session, requestFlowFile, url);

            this.refLogRequest.invoke(this, logger, httpRequest);

            if (httpRequest.body() != null) {
                session.getProvenanceReporter().send(requestFlowFile, url.toExternalForm(), true);
            }

            long startNanos = System.nanoTime();
            Response responseHttp = okHttpClient.newCall(httpRequest).execute();
            Throwable var16 = null;

            try {
                this.refLogResponse.invoke(this, logger, url, responseHttp);
                int statusCode = responseHttp.code();
                String statusMessage = responseHttp.message();
                Map<String, String> statusAttributes = new HashMap();
                statusAttributes.put("invokehttp.status.code", String.valueOf(statusCode));
                statusAttributes.put("invokehttp.status.message", statusMessage);
                statusAttributes.put("invokehttp.request.url", url.toExternalForm());
                statusAttributes.put("invokehttp.response.url", responseHttp.request().url().toString());
                statusAttributes.put("invokehttp.tx.id", txId.toString());
                if (requestFlowFile != null) {
                    requestFlowFile = session.putAllAttributes(requestFlowFile, statusAttributes);
                }

                if (context.getProperty(PROP_ADD_HEADERS_TO_REQUEST).asBoolean() && requestFlowFile != null) {
                    requestFlowFile = session
                            .putAllAttributes(requestFlowFile,
                                    (Map<String, String>) this.refConvertAttributesFromHeaders.invoke(this,
                                            responseHttp));
                }

                boolean outputBodyToRequestAttribute =
                        (!(boolean) this.refIsSuccess.invoke(this, statusCode) || putToAttribute)
                                && requestFlowFile != null;
                boolean outputBodyToResponseContent =
                        (boolean) this.refIsSuccess.invoke(this, statusCode) && !putToAttribute || context
                                .getProperty(PROP_OUTPUT_RESPONSE_REGARDLESS).asBoolean();
                ResponseBody responseBody = responseHttp.body();
                boolean bodyExists = responseBody != null && !context.getProperty(IGNORE_RESPONSE_CONTENT).asBoolean();
                InputStream responseBodyStream = null;
                SoftLimitBoundedByteArrayOutputStream outputStreamToRequestAttribute = null;
                TeeInputStream teeInputStream = null;

                try {
                    responseBodyStream = bodyExists ? responseBody.byteStream() : null;
                    if (responseBodyStream != null && outputBodyToRequestAttribute && outputBodyToResponseContent) {
                        outputStreamToRequestAttribute = new SoftLimitBoundedByteArrayOutputStream(maxAttributeSize);
                        teeInputStream = new TeeInputStream(responseBodyStream, outputStreamToRequestAttribute);
                    }

                    if (outputBodyToResponseContent) {
                        if (requestFlowFile != null) {
                            responseFlowFile = session.create(requestFlowFile);
                        }
                        else {
                            responseFlowFile = session.create();
                        }

                        responseFlowFile = session.putAllAttributes(responseFlowFile, statusAttributes);
                        responseFlowFile = session
                                .putAllAttributes(responseFlowFile,
                                        (Map<String, String>) this.refConvertAttributesFromHeaders.invoke(this,
                                                responseHttp));
                        if (bodyExists) {
                            MediaType contentType = responseBody.contentType();
                            if (contentType != null) {
                                responseFlowFile = session
                                        .putAttribute(responseFlowFile, CoreAttributes.MIME_TYPE.key(),
                                                contentType.toString());
                            }

                            if (teeInputStream != null) {
                                responseFlowFile = session.importFrom(teeInputStream, responseFlowFile);
                            }
                            else {
                                responseFlowFile = session.importFrom(responseBodyStream, responseFlowFile);
                            }

                            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                            if (requestFlowFile != null) {
                                session.getProvenanceReporter().fetch(responseFlowFile, url.toExternalForm(), millis);
                            }
                            else {
                                session.getProvenanceReporter().receive(responseFlowFile, url.toExternalForm(), millis);
                            }
                        }
                    }

                    if (outputBodyToRequestAttribute && bodyExists) {
                        String attributeKey = context.getProperty(PROP_PUT_OUTPUT_IN_ATTRIBUTE)
                                .evaluateAttributeExpressions(requestFlowFile).getValue();
                        if (attributeKey == null) {
                            attributeKey = "invokehttp.response.body";
                        }

                        int size;
                        byte[] outputBuffer;
                        if (outputStreamToRequestAttribute != null) {
                            outputBuffer = outputStreamToRequestAttribute.getBuffer();
                            size = outputStreamToRequestAttribute.size();
                        }
                        else {
                            outputBuffer = new byte[maxAttributeSize];
                            size = StreamUtils.fillBuffer(responseBodyStream, outputBuffer, false);
                        }

                        String bodyString = new String(outputBuffer, 0, size,
                                (Charset) this.refGetCharsetFromMediaType.invoke(this, responseBody.contentType()));
                        requestFlowFile = session.putAttribute(requestFlowFile, attributeKey, bodyString);
                        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                        session.getProvenanceReporter().modifyAttributes(requestFlowFile, "The " + attributeKey
                                + " has been added. The value of which is the body of a http call to " + url
                                .toExternalForm() + ". It took " + millis + "millis,");
                    }
                }
                finally {
                    if (outputStreamToRequestAttribute != null) {
                        outputStreamToRequestAttribute.close();
                    }

                    if (teeInputStream != null) {
                        teeInputStream.close();
                    }
                    else if (responseBodyStream != null) {
                        responseBodyStream.close();
                    }

                }

                this.refRoute.invoke(this, requestFlowFile, responseFlowFile, session, context, statusCode);
            }
            catch (Throwable var49) {
                var16 = var49;
                throw var49;
            }
            finally {
                if (responseHttp != null) {
                    if (var16 != null) {
                        try {
                            responseHttp.close();
                        }
                        catch (Throwable var47) {
                            var16.addSuppressed(var47);
                        }
                    }
                    else {
                        responseHttp.close();
                    }
                }

            }
        }
        catch (Exception var51) {
            if (requestFlowFile != null) {
                logger.error("Routing to {} due to exception: {}", new Object[] { REL_FAILURE.getName(), var51 },
                        var51);
                requestFlowFile = session.penalize(requestFlowFile);
                requestFlowFile = session
                        .putAttribute(requestFlowFile, "invokehttp.java.exception.class", var51.getClass().getName());
                requestFlowFile = session
                        .putAttribute(requestFlowFile, "invokehttp.java.exception.message", var51.getMessage());
                session.transfer(requestFlowFile, REL_FAILURE);
            }
            else {
                logger.error("Yielding processor due to exception encountered as a source processor: {}", var51);
                context.yield();
            }

            if (responseFlowFile != null) {
                session.remove(responseFlowFile);
            }
        }

    }
}
