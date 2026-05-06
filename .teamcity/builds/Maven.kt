package builds

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.toId

open class Maven(
    id: String,
    name: String,
    goals: String,
    args: String? = null,
    javaVersion: JavaVersion = JavaVersion.V_11,
    size: LinuxSize = LinuxSize.SMALL
) :
    BuildType({
      this.id(id.toId())
      this.name = name

      steps {
        runMaven(javaVersion) {
          this.goals = goals
          this.runnerArgs = "$MAVEN_DEFAULT_ARGS ${args ?: ""}"
        }
      }

      features { buildCache(javaVersion) }

      requirements { runOnLinux(size) }
    })
