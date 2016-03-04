resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases"

addSbtPlugin("com.github.hochgi" % "sbt-cassandra-plugin"   % "0.6.2")
addSbtPlugin("de.heikoseeberger" % "sbt-header"             % "1.5.0")
addSbtPlugin("org.scalastyle"    %% "scalastyle-sbt-plugin" % "0.6.0")