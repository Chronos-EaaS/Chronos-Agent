# Chronos Agent

Reference implementation of an agent library written in Java. This library handles all the communication with [Chronos Control](https://github.com/Chronos-EaaS/Chronos-Control) including the upload of the results. It provides an interface with all necessary methods required for adding Chronos support to an existing evaluation client.

## Getting Started

* Chronos Agent is published to Maven Central. Make sure that you have `mavenCentral()` to the `repositories` in your gradle build file.
* Add `implementation group: 'org.chronos-eaas', name: 'chronos-agent', version: '2.3.4'` to your `dependencies`.
* Extend the `AbstractChronosAgent` class, call `YourClass.start()` in your `main` method, and you are good to go!
  > Assuming that you already have a running [Chronos Control](https://github.com/Chronos-EaaS/Chronos-Control/) instance


## Roadmap
See the [open issues](https://github.com/Chronos-EaaS/Chronos-Agent/issues) for a list of proposed features (and known issues).


## Contributing
We highly welcome contributions to the Chronos project. If you would like to contribute, please fork the repository and submit your changes as a pull request.


## License
The MIT License (MIT)
