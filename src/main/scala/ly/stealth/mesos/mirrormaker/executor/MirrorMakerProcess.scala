package ly.stealth.mesos.mirrormaker.executor

import java.util.Properties
import java.util.concurrent.{CountDownLatch, TimeUnit}

import ly.stealth.mesos.mirrormaker.Util
import org.apache.kafka.clients.consumer.{CommitType, KafkaConsumer}
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._

class MirrorMakerProcess(mesosTaskStartedCallback: () => Unit) {

  val logger: Logger = Logger.getLogger(this.getClass)

  @volatile private var exitingOnSendFailure: Boolean = false
  private var producer: MirrorMakerProducer = null
  private val shutdownLatch: CountDownLatch = new CountDownLatch(1)
  private var lastOffsetCommitMs = System.currentTimeMillis()


  def doWork(taskData: String): Unit = {
    val data: Map[String, String] = Util.parseMap(taskData)

    val offsetCommitIntervalMs = data("offset.commit.interval.ms").toLong
    val consumerProps = Util.parseMap(data("consumerProps"))
    val producerProps = Util.parseMap(data("producerProps"))

    val topics = Util.parseList(data("topics")).map {
      case (t, _) => t
    }

    logger.info(s"Starting Mirror Maker for topics: $topics")
    logger.info(s"""Consumer configuration:\n${Util.propsPrettyPrint(consumerProps)}""")
    logger.info(s"""Producer configuration:\n${Util.propsPrettyPrint(consumerProps)}""")
    logger.info(s"""Other settings:\noffset.commit.interval.ms=$offsetCommitIntervalMs""")

    var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null
    try {
      consumer = new KafkaConsumer[Array[Byte], Array[Byte]](Util.mapTopProp(consumerProps))
      consumer.subscribe(topics: _*)

      producer = new MirrorMakerProducer(abortOnSendFailure = true, Util.mapTopProp(producerProps))

      // We assume that by this time mesos task can be considered running
      mesosTaskStartedCallback()

      while (!exitingOnSendFailure) {
        val records = consumer.poll(1000)
        for (record <- records.iterator().asScala) {
          logger.trace("Sending message with value size %d".format(record.value().length))
          producer.send(new ProducerRecord[Array[Byte], Array[Byte]](record.topic, record.key(), record.value()))
          maybeFlushAndCommitOffsets(consumer, offsetCommitIntervalMs)
        }
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        logger.fatal("Mirror maker thread failure due to ", t)
    } finally {
      logger.info("Flushing producer.")
      producer.flush()
      logger.info("Committing consumer offsets.")
      commitOffsets(consumer)
      logger.info("Shutting down consumer connectors.")
      consumer.close()
      shutdownLatch.countDown()
      logger.info("Mirror maker thread stopped")
    }
  }

  def maybeFlushAndCommitOffsets(consumer: KafkaConsumer[Array[Byte], Array[Byte]], offsetCommitIntervalMs: Long) {
    if (System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
      producer.flush()
      commitOffsets(consumer)
      lastOffsetCommitMs = System.currentTimeMillis()
    }
  }

  def commitOffsets(consumer: KafkaConsumer[Array[Byte], Array[Byte]]) {
    if (!exitingOnSendFailure) {
      logger.trace("Committing offsets.")
      consumer.commit(CommitType.SYNC)
    } else {
      logger.info("Exiting on send failure, skip committing offsets.")
    }
  }

  /**
   * Taken from kafka upstream project
   */
  private class MirrorMakerProducer(abortOnSendFailure: Boolean, val producerProps: Properties) {

    val sync = producerProps.getProperty("producer.type", "async").equals("sync")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def send(record: ProducerRecord[Array[Byte], Array[Byte]]) {
      if (sync) {
        this.producer.send(record).get()
      } else {
        this.producer.send(record,
          new MirrorMakerProducerCallback(abortOnSendFailure, record.topic(), record.key(), record.value()))
      }
    }

    def flush() {
      this.producer.flush()
    }

    def close() {
      this.producer.close()
    }

    def close(timeout: Long) {
      this.producer.close(timeout, TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Taken from kafka upstream project
   */
  private class MirrorMakerProducerCallback(abortOnSendFailure: Boolean, topic: String, key: Array[Byte], value: Array[Byte])
    extends ErrorLoggingCallback(topic, key, value, false) {

    override def onCompletion(metadata: RecordMetadata, exception: Exception) {
      if (exception != null) {
        // Use default call back to log error. This means the max retries of producer has reached and message
        // still could not be sent.
        super.onCompletion(metadata, exception)
        // If abort.on.send.failure is set, stop the mirror maker. Otherwise log skipped message and move on.
        if (abortOnSendFailure) {
          logger.info("Closing producer due to send failure.")
          exitingOnSendFailure = true
          producer.close(0)
        }
      }
    }
  }
}
