package sample.cluster.transformation
object TransformationApp {
  def main(args: Array[String]): Unit = {

//    TransformationFrontend.main(Seq("2551").toArray)
//    TransformationBackend.main(Seq("2552").toArray)

    // starting 2 frontend nodes and 3 backend nodes
    TransformationFrontend.main(Seq("seed1").toArray)
    TransformationBackend.main(Seq("seed2").toArray)
    TransformationBackend.main(Array.empty)
//    TransformationBackend.main(Array.empty)
//    TransformationBackend.main(Array.empty)
    TransformationFrontend.main(Array.empty)
  }
}