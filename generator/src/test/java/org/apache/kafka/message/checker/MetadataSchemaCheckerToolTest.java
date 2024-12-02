/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.message.checker;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.util.FileUtils;
import org.eclipse.jgit.util.SignatureUtils;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Scanner;

import static org.apache.kafka.message.checker.CheckerTestUtils.messageSpecStringToTempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetadataSchemaCheckerToolTest {
    @Test
    public void testVerifyEvolutionGit() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            Path tempDirectory = Files.createTempDirectory(Paths.get("").toAbsolutePath(), "temp-dir");
            tempDirectory.toFile().deleteOnExit();
            File file = new File(tempDirectory.toAbsolutePath() + "/kafka/metadata/src/main/resources/common/metadata/");
            file.mkdirs();
            deleteEverythingOnExit(tempDirectory);
            setUpGitRepo(file);
            MetadataSchemaCheckerTool.run(new String[]{"verify-evolution-git", "--file", "AbortTransactionRecord.json", "--directoryPath", tempDirectory.toAbsolutePath() + "/kafka/metadata/src/main/resources/common/metadata/"}, new PrintStream(stream));
            assertEquals("Successfully verified evolution of file: AbortTransactionRecord.json",
                stream.toString().trim());
        }

    }

    @Test
    public void testSuccessfulParse() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            String path = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}");
            MetadataSchemaCheckerTool.run(new String[] {"parse", "--path", path}, new PrintStream(stream));
            assertEquals("Successfully parsed file as MessageSpec: " + path, stream.toString().trim());
        }
    }

    @Test
    public void testSuccessfulVerifyEvolution() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            String path = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}");
            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                "--path1", path, "--path2", path}, new PrintStream(stream));
            assertEquals("Successfully verified evolution of path1: " + path + ", and path2: " + path,
                stream.toString().trim());
        }
    }

    private void setUpGitRepo(File localPath) throws Exception {
        Path path = getKafkaDirectory(localPath.toPath());

        // Initialize a new repository
        Git git = Git.init().setDirectory(path.toFile()).call();
        Path hiddenGitPath = Paths.get(path + "/.git");
        hiddenGitPath.toFile().deleteOnExit();

        // Create the file
        String AbortTransactionRecordCopyPath = getKafkaDirectory(Paths.get("").toAbsolutePath()).toString() + "/metadata/src/main/resources/common/metadata/AbortTransactionRecord.json";
        String AbortTransactionRecordCopy = localPath + "/AbortTransactionRecord.json";
        Files.copy(Paths.get(AbortTransactionRecordCopyPath) , Paths.get(AbortTransactionRecordCopy));
        Paths.get(AbortTransactionRecordCopy).toFile().deleteOnExit();

        // Add the file to the repository and commit it
        git.add().addFilepattern("metadata/src/main/resources/common/metadata/AbortTransactionRecord.json").call();
        deleteEverythingOnExit(hiddenGitPath);
        git.commit().setMessage("Initial commit")
                .call();
        deleteEverythingOnExit(hiddenGitPath);
        git.branchRename().setNewName("trunk").call();
        deleteEverythingOnExit(hiddenGitPath);
    }

    private Path getKafkaDirectory(Path path) {
        while (!path.endsWith("kafka")) {
            path = path.getParent();
            if (path == null) {
                throw new RuntimeException("Invalid directory, need to be within the kafka directory");
            }
        }
        return path;
    }

    private void deleteEverythingOnExit(Path root) {
        File[] directories = root.toFile().listFiles();
        if (directories == null) {
            throw new RuntimeException("listFiles returned null: " + root);
        }
        for (File i : directories) {
            i.deleteOnExit();
            if (i.isDirectory()) {
                deleteEverythingOnExit(i.toPath());
            }
        }

    }
}
