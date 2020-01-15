package tech.mlsq.streambootstrapatstartup

import _root_.streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import _root_.streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import org.apache.spark.sql.SparkSession
import tech.mlsql.ets.{SchedulerCommand, ScriptRunner}
import tech.mlsql.job.{JobManager, MLSQLJobType}
import tech.mlsql.version.VersionCompatibility

/**
  * 2019-09-20 WilliamZhu(allwefantasy@gmail.com)
  */
class StreamApp extends tech.mlsql.app.App with VersionCompatibility {


  override def run(args: Seq[String]): Unit = {
    val root = runtime.sparkSession
    import SchedulerCommand._
    import root.implicits._

    val streams = tryReadTable(root, StreamAppConfig.TABLE, () => root.createDataset[Stream](Seq()).toDF())
    streams.as[Stream].collect().foreach { stream =>
      val session = getSessionByOwner(stream.owner)
      val job = JobManager.getJobInfo(stream.owner, stream.name, MLSQLJobType.STREAM, stream.content, -1)
      setUpScriptSQLExecListener(stream.owner, session, job.groupId, stream.home)
      ScriptRunner.runJob(stream.content, job, (df) => {

      })
    }
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
  val TABLE = "__mlsql__.streambootstrapatstartup_streams"
}

case class Stream(name: String, content: String, owner: String, home: String)
