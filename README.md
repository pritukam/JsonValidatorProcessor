# JsonValidatorProcessor
Nifi processor that takes in Json Schema and validates flowfiles accordingly

This uses the ever-it json validator, as that apparently, is the fastest validator available. 

This is a very light processor that takes in a JSON schema as defined by http://json-schema.org/

Shout out to http://www.nifi.rocks/ for their article on creating processors.

The latest version of nifi, 1.4.0 has validator for JSON.

This was created a month before the version went live.

#How to use.

1. Clone this repo and build the project using maven.

2. Put the .nar file in $NIFI_HOME/lib

Troubleshooting:

If following the steps doesn't work, then in the unpacked folder of nifi, find the processor create a file with the name "/src/main/resources/META-INF/services/org.apache.nifi.processor.Processor" and add the fully qualified name of the processor class, in this case "org.apache.nifi.processors.JsonValidator"

ping me anytime for any help :)


