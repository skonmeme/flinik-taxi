package com.skt.skon.taxi

import com.skt.skon.taxi.sources.TaxiRideSource
import com.skt.skon.taxi.utils.GeoUtils
import org.apache.flink.streaming.api.scala._

/**
  * Skeleton for a Flink Streaming Job.
  *
  * For a tutorial how to write a Flink streaming application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
  *
  * To package your application into a JAR file for execution, run
  * 'mvn clean package' on the command line.
  *
  * If you change the name of the main class (with the public static void main(String[] args))
  * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
  */
object RideCleansing {

  def main(args: Array[String]): Unit = {
    // data stream speed
    val servingSpeedFactor = 10

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val cleanRideStream = env
      .addSource(new TaxiRideSource("/Users/skon/Documents/src/skonmeme/da/nycTaxiRides.gz", servingSpeedFactor))
      .filter(ride => GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat))

    cleanRideStream.print

    // execute program
    env.execute("TaxiRide Cleansing")
  }

}
