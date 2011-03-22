Talk on 'Getting Started with Hadoop'
===

This repo tracks all source code, data samples and visualization examples for my talks on 'Getting Started with Hadoop'.

Data and Visualizations
---

Data can be found in the `data` directory including raw logs courtesy of [@maxheadroom](http://www.twitter.com/maxheadroom) from [mac-geeks.de](http://www.mac-geeks.de). The final results of the Apache access log throughput script are also loaded up into a [Google Docs spreadsheet](https://spreadsheets.google.com/ccc?key=0AqAx7w3Zbl99dHFwX2l0TU4wQ2tWYWNZTG1DOENOTVE&hl=en) for ad-hoc analysis and visualization. The `src/html` directory contains a simple page with example visualizations using [Google Chart API's](http://code.google.com/apis/chart) [time series chart](http://code.google.com/apis/visualization/documentation/gallery/annotatedtimeline.html).

Dependencies
---

All dependencies are managed currently in the Maven `pom`. There are a few, however, that need to be added to your repository (local or otherwise) before continuing. They are all documented inline in the `dependencies` of the Maven `pom`. We assume that you have installed Pig and have the `$PIG_HOME` environment variable defined (it's referenced in the `pom` comments on installing local dependencies).

Quirks
---

Unit testing of Pig is currently done using PigUnit, a new xUnit style testing harness released in Pig 0.8.0. Since this is it's debut there are a few quirks to be aware of:

 * runs create two working files in the root directory: `pigunit-input-overriden.txt`, `.pigunit-input-overriden.txt.crc`
 * heap space is at a premium when running the Pig scripts, so max heap space JVM parameters need to be set (to something like`-Xmx1024m`)
   * running tests from Maven: configured in the Maven `pom` to run the Surefire plugin with a fixed max heap space setting
   * running tests from Eclipse: use the [JUnit Lanch Fixer](http://code.google.com/p/junitlaunchfixer) plugin and set the max heap space for all JUnit executions automatically
