pipeline {
  agent {
    dockerfile true
  }
  stages {
    stage('Test Pipeline') {
      steps {
        echo 'Hello Dockerfile'
        sh 'sbt clean test coverage coverageReport'
        junit 'target/test-reports/*.xml'
      }
    }
    stage('Build Fat Jar') {
      steps {
        sh 'sbt clean compile package'
      }
    }
    stage('Deploy') {
      steps {
        archiveArtifacts 'target/scala-2.11/*'
      }
    }
  }
}