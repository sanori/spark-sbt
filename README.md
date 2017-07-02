# Spark development with SBT

This is an example scala sbt configuration that shows `sbt console` works like as `spark-shell`. You don't need to install Spark to use spark-shell if you are developing with [sbt](http://www.scala-sbt.org/).

## How to try

```
git clone https://github.com/sanori/spark-sbt
cd spark-sbt
sbt console
```

The first time to run `sbt console`, it may take several minutes or almost an hour to download Spark modules.
