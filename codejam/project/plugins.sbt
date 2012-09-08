addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.1.0")

resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
)

libraryDependencies ++= Seq(
   "com.google.caliper" % "caliper" % "1.0-SNAPSHOT"
)

