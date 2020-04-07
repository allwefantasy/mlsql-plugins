package tech.mlsq.streambootstrapatstartup

import _root_.streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import _root_.streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import org.apache.spark.sql.SparkSession
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.ScriptRunner
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.job.{JobManager, MLSQLJobType}
import tech.mlsql.store.DBStore
import tech.mlsql.version.VersionCompatibility

/**
 * 2019-09-20 WilliamZhu(allwefantasy@gmail.com)
 */
class StreamApp extends tech.mlsql.app.App with VersionCompatibility with Logging {


  override def run(args: Seq[String]): Unit = {
    val root = runtime.sparkSession
    import root.implicits._

    ETRegister.register("StreamPersistCommand", classOf[StreamPersistCommand].getName)
    CommandCollection.refreshCommandMapping(Map("streamPersist" -> "StreamPersistCommand"))

    val thread = new Thread("start MLSQL stream") {
      override def run(): Unit = {
        while (!PlatformManager.RUNTIME_IS_READY.get()) {
          Thread.sleep(3000)
          logInfo("Waiting MLSQL runtime ready to start streams.")
        }
        logInfo("Starting to start streams.")
        val streams = DBStore.store.tryReadTable(root, StreamAppConfig.TABLE, () => root.createDataset[Stream](Seq()).toDF())
        streams.as[Stream].collect().foreach { stream =>
          val session = getSessionByOwner(stream.owner)
          val job = JobManager.getJobInfo(stream.owner, stream.name, MLSQLJobType.STREAM, stream.content, -1)
          setUpScriptSQLExecListener(stream.owner, session, job.groupId, stream.home)
          ScriptRunner.runJob(stream.content, job, (df) => {

          })
        }
      }
    }
    thread.start()

  }

  def setUpScriptSQLExecListener(owner: String, sparkSession: SparkSession, groupId: String, home: String) = {
    val context = new ScriptSQLExecListener(sparkSession, "", Map[String, String](owner -> home))
    ScriptSQLExec.setContext(new MLSQLExecuteContext(context, owner, context.pathPrefix(None), groupId, Map()))
    context.addEnv("SKIP_AUTH", "true")
    context.addEnv("HOME", context.pathPrefix(None))
    context.addEnv("OWNER", owner)
    context
  }

  def getSessionByOwner(owner: String) = {
    runtime.getSession(owner)
  }

  def runtime = {
    PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
  }

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }
}

object StreamAppConfig {
  val TABLE = "__mlsql__.streams"
}

case class Stream(name: String, content: String, owner: String, home: String)

object StreamApp {
}
