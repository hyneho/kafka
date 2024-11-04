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
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.security.KeyPair;

import static org.apache.kafka.message.checker.CheckerUtils.GetDataFromGit;

public class MetadataSchemaCheckerTool {
    public static void main(String[] args) throws Exception {
        try {
            run(args, System.out);
        } catch (HelpScreenException e) {
        }
    }

    public static void run(
        String[] args,
        PrintStream writer
    ) throws Exception {
        ArgumentParser argumentParser = ArgumentParsers.
            newArgumentParser("metadata-schema-checker").
            defaultHelp(true).
            description("The Kafka metadata schema checker tool.");
        Subparsers subparsers = argumentParser.addSubparsers().dest("command");
        Subparser parseParser = subparsers.addParser("parse").
            help("Verify that a JSON file can be parsed as a MessageSpec.");
        parseParser.addArgument("--path", "-p").
            required(true).
            help("The path to a schema JSON file.");
        Subparser evolutionVerifierParser = subparsers.addParser("verify-evolution").
            help("Verify that an evolution of a JSON file is valid.");
        evolutionVerifierParser.addArgument("--path1", "-1").
            required(true).
            help("The initial schema JSON path.");
        evolutionVerifierParser.addArgument("--path2", "-2").
            required(true).
            help("The final schema JSON path.");
        Subparser evolutionGitVerifierParser = subparsers.addParser("verify-evolution-git").
            help(" Verify that an evolution of a JSon file is valid using git. ");
        evolutionGitVerifierParser.addArgument("--file", "-3").
            required(true).
            help("The edited json file");
        Namespace namespace;
        if (args.length == 0) {
            namespace = argumentParser.parseArgs(new String[] {"--help"});
        } else {
            namespace = argumentParser.parseArgs(args);
        }
        String command = namespace.getString("command");
        switch (command) {
            case "parse": {
                String path = namespace.getString("path");
                CheckerUtils.readMessageSpecFromFile(path);
                writer.println("Successfully parsed file as MessageSpec: " + path);
                break;
            }
            case "verify-evolution": {
                String path1 = namespace.getString("path1");
                String path2 = namespace.getString("path2");
                EvolutionVerifier verifier = new EvolutionVerifier(
                    CheckerUtils.readMessageSpecFromFile(path1),
                    CheckerUtils.readMessageSpecFromFile(path2));
                verifier.verify();
                writer.println("Successfully verified evolution of path1: " + path1 +
                        ", and path2: " + path2);
                break;
            }
            case "verify-evolution-git": {
                String fileCheckMetadata = namespace.getString("file");
                String gitContent = GetDataFromGit(fileCheckMetadata);
                EvolutionVerifier verifier = new EvolutionVerifier(
                        CheckerUtils.readMessageSpecFromFile(Paths.get("").toAbsolutePath().getParent() + "/metadata/src/main/resources/common/metadata/" + fileCheckMetadata),
                        CheckerUtils.readMessageSpecFromString(gitContent));
                verifier.verify();writer.println("Successfully verified evolution of file: " + fileCheckMetadata);
                break;
            }
            default:
                throw new RuntimeException("Unknown command " + command);
        }
    }

}
