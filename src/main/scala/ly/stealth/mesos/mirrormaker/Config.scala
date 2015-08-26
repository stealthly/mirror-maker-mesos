package ly.stealth.mesos.mirrormaker

import java.io.File
import java.util.Properties

import org.apache.log4j.Logger

object Config {
  private val logger = Logger.getLogger(this.getClass)

  def apply(file: File, overrides: Map[String, String]): Config = {
    val props = Util.loadConfigFile(file)

    overrides.foreach {
      case (k, v) =>
        logger.info(s"Config setting override: $k=$v")
        props.setProperty(k, v)
    }

    new Config(props)
  }

  def apply(file: File): Config = apply(file, Map.empty)
}


class Config(props: Properties) {

  val MesosMaster = props.getProperty("mesos.master")
  val FrameworkName = props.getProperty("mesos.framework.name")

  val RestApi = props.getProperty("mmm.api")

  val Topics = props.getProperty("mmm.topics").split(",").map(_.trim).toSeq

  val OffsetCommitIntervalMs = props.getProperty("mmm.offset.commit.interval.ms")

}
