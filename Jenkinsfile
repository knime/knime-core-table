#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2022-06'

library "knime-pipeline@$BN"

properties([
    // provide a list of upstream jobs which should trigger a rebuild of this job
    pipelineTriggers([
        upstream("knime-tp/${env.BRANCH_NAME.replaceAll('/', '%2F')}")
    ]),
    parameters([p2Tools.getP2pruningParameter()]),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    // provide the name of the update site project
    knimetools.defaultTychoBuild('org.knime.update.core.table')

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        workflowTests.runSonar()
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}
/* vim: set shiftwidth=4 expandtab smarttab: */
