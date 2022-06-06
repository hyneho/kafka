/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
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

def doValidation() {
  sh """
    ./gradlew -PscalaVersion=$SCALA_VERSION clean compileJava compileScala  \
        rat \
        --profile --no-daemon --continue -PxmlSpotBugsReport=true
  """
}

def isChangeRequest(env) {
  env.CHANGE_ID != null && !env.CHANGE_ID.isEmpty()
}

def retryFlagsString(env) {
    if (isChangeRequest(env)) " -PmaxTestRetries=1 -PmaxTestRetryFailures=5"
    else ""
}

def doTest(env, target = "streams:unitTest") {
  sh """./gradlew -PscalaVersion=$SCALA_VERSION ${target} \
      --profile --no-daemon --continue -PtestLoggingEvents=started,passed,skipped,failed \
      -PignoreFailures=true -PmaxParallelForks=2""" + retryFlagsString(env)
  junit '**/build/test-results/**/TEST-*.xml'
}


pipeline {
  agent none
  
  options {
    disableConcurrentBuilds()
  }
  
  stages {
    stage('Build') {
      parallel {

        stage('JDK 8 and Scala 2.12') {
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_1.8_latest'
            maven 'maven_3_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
          environment {
            SCALA_VERSION=2.12
          }
          steps {
            doValidation()
            doTest(env)
          }
        }

        stage('JDK 11 and Scala 2.13') {
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_11_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
          environment {
            SCALA_VERSION=2.13
          }
          steps {
            doValidation()
            doTest(env)
            echo 'Skipping Kafka Streams archetype test for Java 11'
          }
        }

        stage('JDK 17 and Scala 2.13') {
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_17_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
          environment {
            SCALA_VERSION=2.13
          }
          steps {
            doValidation()
            doTest(env)
            echo 'Skipping Kafka Streams archetype test for Java 17'
          }
        }

        stage('ARM') {
          options {
            timestamps()
          }
          environment {
            SCALA_VERSION=2.12
          }
          stages {
            stage('Check ARM Agent') {
              agent { label 'arm4' }
              options {
                timeout(time: 5, unit: 'MINUTES')
              }
              steps {
                echo 'ARM ok'
              }
            }
            stage('Run ARM Build') {
              agent { label 'arm4' }
              options {
                timeout(time: 2, unit: 'HOURS')
              }
              steps {
                doValidation()
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                  doTest(env, 'unitTest')
                }
                echo 'Skipping Kafka Streams archetype test for ARM build'
              }
            }
          }
        }

        stage('PowerPC') {
          options {
            timestamps()
          }
          environment {
            SCALA_VERSION=2.12
          }
          stages {
            stage('Check PowerPC Agent') {
              agent { label 'ppc64le' }
              options {
                timeout(time: 5, unit: 'MINUTES')
              }
              steps {
                echo 'PowerPC ok'
              }
            }
            stage('Run PowerPC Build') {
              agent { label 'ppc64le' }
              options {
                timeout(time: 2, unit: 'HOURS')
              }
              steps {
                doValidation()
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                  doTest(env, 'unitTest')
                }
                echo 'Skipping Kafka Streams archetype test for PowerPC build'
              }
            }
          }
        }
        
        // To avoid excessive Jenkins resource usage, we only run the stages
        // above at the PR stage. The ones below are executed after changes
        // are pushed to trunk and/or release branches. We achieve this via
        // the `when` clause.
        
        stage('JDK 8 and Scala 2.13') {
          when {
            not { changeRequest() }
            beforeAgent true
          }
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_1.8_latest'
            maven 'maven_3_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
          environment {
            SCALA_VERSION=2.13
          }
          steps {
            doValidation()
            doTest(env)
            tryStreamsArchetype()
          }
        }

        stage('JDK 11 and Scala 2.12') {
          when {
            not { changeRequest() }
            beforeAgent true
          }
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_11_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
          environment {
            SCALA_VERSION=2.12
          }
          steps {
            doValidation()
            doTest(env)
            echo 'Skipping Kafka Streams archetype test for Java 11'
          }
        }

        stage('JDK 17 and Scala 2.12') {
          when {
            not { changeRequest() }
            beforeAgent true
          }
          agent { label 'ubuntu' }
          tools {
            jdk 'jdk_17_latest'
          }
          options {
            timeout(time: 8, unit: 'HOURS') 
            timestamps()
          }
          environment {
            SCALA_VERSION=2.12
          }
          steps {
            doValidation()
            doTest(env)
            echo 'Skipping Kafka Streams archetype test for Java 17'
          }
        }
      }
    }
  }
  
  post {
    always {
      node('ubuntu') {
        script {
          if (!isChangeRequest(env)) {
            step([$class: 'Mailer',
                 notifyEveryUnstableBuild: true,
                 recipients: "dev@kafka.apache.org",
                 sendToIndividuals: false])
          }
        }
      }
    }
  }
}
