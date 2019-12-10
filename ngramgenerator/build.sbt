
scalaVersion     := "2.12.8"
version          := "0.1.0"

val sparkVersion = "2.4.3"

name := "ngramgenerator"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % sparkVersion,
    	"org.apache.spark" %% "spark-core" % sparkVersion
		)


