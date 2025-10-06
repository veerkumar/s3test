/*
 *
 *  Copyright 2025 Datadobi
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
package com.datadobi.s3test;

import com.datadobi.s3test.s3.S3TestBase;
import com.datadobi.s3test.s3.ServiceDefinition;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class RunTests {
    public static void main(String[] args) throws InitializationError, IOException {
        String include = null;
        String exclude = null;

        int i = 0;
        for (; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("-")) {
                break;
            }

            if (arg.equals("-e") || arg.equals("--exclude")) {
                exclude = args[++i];
            } else if (arg.equals("-i") || arg.equals("--include")) {
                include = args[++i];
            }
        }

        if (i == args.length) {
            System.err.println("Usage: RunTests [options] S3_URI");
            System.err.println("Options:");
            System.err.println("  -e --exclude PATTERN    Exclude test matching pattern");
            System.err.println("  -i --include PATTERN    Include test matching pattern");
            System.exit(1);
        }

        var target = ServiceDefinition.fromURI(args[i++]);

        S3TestBase.DEFAULT = target;

        System.out.println("S3 tests: " + target.host());
        System.out.println();

        List<Class<?>> classes = new ArrayList<>();

        classes.add(ChecksumTests.class);
        classes.add(ConditionalRequestsTest.class);
        classes.add(DeleteObjectsTests.class);
        classes.add(DeleteObjectTests.class);
        classes.add(GetObjectTests.class);
        classes.add(ListBucketsTests.class);
        classes.add(ListObjectsTests.class);
        classes.add(MultiPartUploadTests.class);
        classes.add(ObjectKeyTests.class);
        classes.add(PrefixDelimiterTests.class);
        classes.add(PutObjectTests.class);

        JUnitCore junit = new JUnitCore();
        junit.addListener(new TextListener());

        Pattern excludePattern = exclude == null ? null : Pattern.compile(exclude);
        Pattern includePattern = include == null ? null : Pattern.compile(include);

        for (Class<?> c : classes) {
            BlockJUnit4ClassRunner runner = new BlockJUnit4ClassRunner(c);

            try {
                runner.filter(new Filter() {
                    @Override
                    public boolean shouldRun(Description description) {
                        String methodName = description.getMethodName();
                        if (excludePattern != null && excludePattern.matcher(methodName).matches()) {
                            System.out.println(methodName + " excluded");
                            return false;
                        }

                        if (includePattern != null && !includePattern.matcher(methodName).matches()) {
                            System.out.println(methodName + " not included");
                            return false;
                        }

                        return true;
                    }

                    @Override
                    public String describe() {
                        return "Name filter";
                    }
                });
                junit.run(runner);
            } catch (NoTestsRemainException e) {
                System.out.println("Skipping " + runner.getDescription());
            }
        }
    }

    private static class TextListener extends RunListener {
        private final PrintStream stdOut;
        private Failure failure;
        private boolean ignored;

        public TextListener() {
            stdOut = System.out;
        }

        @Override
        public void testRunStarted(Description description) throws Exception {
            stdOut.println("Running " + description);
        }

        public void testStarted(Description description) {
            stdOut.append("  " + description.getMethodName());
            failure = null;
            ignored = false;
        }

        public void testFailure(Failure failure) {
            this.failure = failure;
        }

        public void testIgnored(Description description) {
            ignored = true;
        }

        @Override
        public void testFinished(Description description) throws Exception {
            if (ignored) {
                stdOut.println(" 🙈");
            } else if (failure != null) {
                stdOut.println(" ❌");
                stdOut.println(failure.getTrimmedTrace());
            } else {
                stdOut.println(" ✅");
            }
        }
    }
}
