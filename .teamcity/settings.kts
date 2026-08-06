import builds.Build
import builds.DEFAULT_BRANCH
import builds.Neo4jCdcClientVcs
import builds.NightlyBuild
import jetbrains.buildServer.configs.kotlin.project
import jetbrains.buildServer.configs.kotlin.version

version = "2025.11"

project {
  params {
    text("osssonatypeorg-username", "%publish-username%")
    password("osssonatypeorg-password", "%publish-password%")
    password("signing-key-passphrase", "%publish-signing-key-password%")
    password("github-commit-status-token", "%github-token%")
    password("github-pull-request-token", "%github-token%")
    password("semgrep-app-token", "%semgrep-token%")
  }

  vcsRoot(Neo4jCdcClientVcs)

  subProject(
      Build(
          name = "main",
          branchFilter =
              buildString {
                appendLine("+:$DEFAULT_BRANCH")
                appendLine("+:refs/heads/$DEFAULT_BRANCH")
              },
          triggerRules =
              """
                -:comment=^build.*release version.*:**
                -:comment=^build.*update version.*:**
              """
                  .trimIndent(),
          forPullRequests = false))
  subProject(
      Build(
          name = "pull-request",
          branchFilter =
              buildString {
                appendLine("+:pull/*")
                appendLine("+:refs/heads/pull/*")
              },
          forPullRequests = true))
  subProject(NightlyBuild("nightly"))
}
