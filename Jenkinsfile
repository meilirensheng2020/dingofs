pipeline {
  agent any
  stages {
    stage('Build DingoFS') {
      steps {
        sh '''echo "Building DingoFS..."
'''
      }
    }

    stage('Sync Topology Information') {
      steps {
        sh '''echo "Syncing topology information using ${TOPOLOGY_FILE}..."
'''
      }
    }

  }
}