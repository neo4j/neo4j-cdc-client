package builds

import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.sequential
import jetbrains.buildServer.configs.kotlin.toId
import jetbrains.buildServer.configs.kotlin.triggers.schedule
import jetbrains.buildServer.configs.kotlin.triggers.vcs

class NightlyBuild(
    name: String,
    branchFilter: String = buildString {
      appendLine("+:$DEFAULT_BRANCH")
      appendLine("+:refs/heads/$DEFAULT_BRANCH")
    }
) :
    Project({
      this.id(name.toId())
      this.name = name

      val complete = Empty("${name}-complete", "complete")

      val bts = sequential {
        dependentBuildType(SemgrepCheck("${name}-semgrep-check", "semgrep check"))
        dependentBuildType(complete)
      }

      bts.buildTypes().forEach {
        it.thisVcs(DEFAULT_BRANCH)

        it.features {
          loginToECR()
          enableCommitStatusPublisher()
        }

        buildType(it)
      }

      complete.triggers {
        vcs {
          enabled = false
          this.branchFilter = branchFilter
        }

        schedule {
          this.branchFilter = branchFilter
          schedulingPolicy = daily {
            hour = 7
            minute = 0
          }
          triggerBuild = always()
          withPendingChangesOnly = false
          enforceCleanCheckout = true
          enforceCleanCheckoutForDependencies = true
        }
      }
    })
