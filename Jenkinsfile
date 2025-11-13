#!groovy

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

pipeline {

    agent {
        label {
            label params.nodeLabel
        }
    }

    tools {
        // ... tell Jenkins what java version, maven version or other tools are required ...
        maven 'maven_3_latest'
        jdk params.jdkVersion
    }

    options {
        // Configure an overall timeout for the build of ten hours.
        timeout(time: 20, unit: 'HOURS')
        // When we have test-fails e.g. we don't need to run the remaining steps
        buildDiscarder(logRotator(numToKeepStr: '5', artifactNumToKeepStr: '5'))
        disableConcurrentBuilds()
    }

    parameters {
        choice(name: 'nodeLabel', choices: ['ubuntu', 's390x', 'arm', 'Windows']) 
        choice(name: 'jdkVersion', choices: ['jdk_17_latest', 'jdk_21_latest', 'jdk_25_latest', 'jdk_17_latest_windows', 'jdk_21_latest_windows', 'jdk_25_latest_windows'])
        booleanParam(name: 'deployEnabled', defaultValue: false)
        booleanParam(name: 'parallelTestsEnabled', defaultValue: true)
        booleanParam(name: 'sonarEnabled', defaultValue: false)
        booleanParam(name: 'testsEnabled', defaultValue: true)
    }

    stages {
        stage('Initialization') {
            steps {
                echo "running on ${env.NODE_NAME}"
                echo 'Building branch ' + env.BRANCH_NAME
                echo 'Using PATH ' + env.PATH
            }
        }

        stage('Cleanup') {
            steps {
                echo 'Cleaning up the workspace'
                deleteDir()
            }
        }

        stage('Checkout') {
            steps {
                echo 'Checking out branch ' + env.BRANCH_NAME
                checkout scm
            }
        }

        stage('Build JDK 25') {
            tools {
                jdk "jdk_25_latest"
            }
            steps {
                echo 'Building JDK 25'
                sh 'java -version'
                sh 'mvn -version'
                sh 'mvn -U -B -e clean install -DskipTests'
            }
        }

        stage('Build JDK 21') {
            tools {
                jdk "jdk_21_latest"
            }
            steps {
                echo 'Building JDK 21'
                sh 'java -version'
                sh 'mvn -version'
                sh 'mvn -U -B -e clean install -DskipTests'
            }
        }

        stage('Build JDK 17') {
            tools {
                jdk "jdk_17_latest"
            }
            steps {
                echo 'Building JDK 17'
                sh 'java -version'
                sh 'mvn -version'
                sh 'mvn -U -B -e clean install -DskipTests'
            }
        }

        stage('Verify') {
            tools {
                jdk params.jdkVersion
            }
            steps {
                echo 'Running apache-rat:check'
                sh 'java -version'
                sh 'mvn -version'
                sh 'mvn apache-rat:check'
            }
        }

        stage('Tests') {
            tools {
                jdk params.jdkVersion
            }
            when { expression { return params.testsEnabled } }
            steps {
                sh 'java -version'
                sh 'mvn -version'

                // all tests is very very long (10 hours on Apache Jenkins)
                // sh 'mvn -B -e test -pl activemq-unit-tests -Dactivemq.tests=all'
                script {
                    if (params.parallelTestsEnabled == 'true') {
                        sh 'echo "Running parallel-tests ..."'
                        sh 'mvn -B -e -fae -Pparallel-tests test -Dsurefire.rerunFailingTestsCount=3'
                    } else {
                        sh 'echo "Running tests ..."'
                        sh 'mvn -B -e -fae test -Dsurefire.rerunFailingTestsCount=3'
                    }
                }
            }
            post {
                always {
                    junit(testResults: '**/surefire-reports/*.xml', allowEmptyResults: true)
                    junit(testResults: '**/failsafe-reports/*.xml', allowEmptyResults: true)
                }
            }
        }

        stage('Deploy') {
            tools {
                jdk params.jdkVersion
            }
            when {
                expression {
                    params.deployEnabled && env.BRANCH_NAME ==~ /(activemq-5.19.x|main)/
                }
            }
            steps {
                echo 'Deploying'
                sh 'java -version'
                sh 'mvn -version'
                sh 'mvn -B -e deploy -Pdeploy -DskipTests'
            }
        }

        stage('Quality') {
            when { expression { return params.sonarEnabled } }

            steps {
                withCredentials([string(credentialsId: 'SONARCLOUD_TOKEN', variable: 'SONAR_TOKEN')]) {
                  sh 'echo "Running the Sonar stage"'
                  sh 'mvn -B -e -fae clean verify sonar:sonar -Dsonar.projectKey=apache_activemq -Dsonar.organization=apache -Dsonar.host.url=https://sonarcloud.io -Dsonar.login=${SONAR_TOKEN} -Dsurefire.rerunFailingTestsCount=3'
                }
            }
        }

    }

    // Do any post build stuff ... such as sending emails depending on the overall build result.
    post {
        // If this build failed, send an email to the list.
        failure {
            script {
                if(env.BRANCH_NAME == "activemq-5.19.x" || env.BRANCH_NAME == "main") {
                    emailext(
                            subject: "[BUILD-FAILURE]: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]'",
                            body: """
BUILD-FAILURE: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]':
Check console output at "<a href="${env.BUILD_URL}">${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]</a>"
""",
                            to: "commits@activemq.apache.org",
                            recipientProviders: [[$class: 'DevelopersRecipientProvider']]
                    )
                }
            }
        }

        // If this build didn't fail, but there were failing tests, send an email to the list.
        unstable {
            script {
                if(env.BRANCH_NAME == "activemq-5.19.x" || env.BRANCH_NAME == "main") {
                    emailext(
                            subject: "[BUILD-UNSTABLE]: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]'",
                            body: """
BUILD-UNSTABLE: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]':
Check console output at "<a href="${env.BUILD_URL}">${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]</a>"
""",
                            to: "commits@activemq.apache.org",
                            recipientProviders: [[$class: 'DevelopersRecipientProvider']]
                    )
                }
            }
        }

        // Send an email, if the last build was not successful and this one is.
        success {
            // Cleanup the build directory if the build was successful
            // (in this cae we probably don't have to do any post-build analysis)
            deleteDir()
            script {
                if ((env.BRANCH_NAME == "activemq-5.19.x" || env.BRANCH_NAME == "main") && (currentBuild.previousBuild != null) && (currentBuild.previousBuild.result != 'SUCCESS')) {
                    emailext (
                            subject: "[BUILD-STABLE]: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]'",
                            body: """
BUILD-STABLE: Job '${env.JOB_NAME} [${env.BRANCH_NAME}] [${env.BUILD_NUMBER}]':
Is back to normal.
""",
                            to: "commits@activemq.apache.org",
                            recipientProviders: [[$class: 'DevelopersRecipientProvider']]
                    )
                }
            }
        }
    }

}
