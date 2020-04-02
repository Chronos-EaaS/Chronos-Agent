# Chronos Agent

Reference implementation of an agent library written in Java. This library handles all the communication with [Chronos Control](https://github.com/Chronos-EaaS/Chronos-Control) including the upload of the results. It provides an interface with all necessary methods required for adding Chronos support to an existing evaluation client.

## Getting Started
  * Add `maven { url 'https://dbis-nexus.dmi.unibas.ch/repository/maven2/' }` to the `repositories` in your gradle build file.
  * Add `implementation group: 'ch.unibas.dmi.dbis.chronos', name: 'chronos-agent', version: '2.3.1'` to your `dependencies`.
  * Extend the `AbstractChronosAgent` class, call `YourClass.start()` in your `main` method, and you are good to go!
    > Assuming that you already have a running [Chronos Control](https://github.com/Chronos-EaaS/Chronos-Control/) instance :smirk:)


>**Fat Jar** &nbsp;&nbsp; If you require a fat jar, please clone the repository and run `gradlew shadowJar`. The fat jar is located in `build/libs/`. You need at least Java 8 for building. 

## Roadmap
See the [open issues](https://github.com/Chronos-EaaS/Chronos-Agent/issues) for a list of proposed features (and known issues).


## Contributing
We highly welcome contributions to the Chronos project. If you would like to contribute, please fork the repository and submit your changes as a pull request.


## License
The MIT License (MIT)
