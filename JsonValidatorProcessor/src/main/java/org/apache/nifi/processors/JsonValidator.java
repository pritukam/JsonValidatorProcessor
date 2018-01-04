package org.apache.nifi.processors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Pritish on 30-11-2017.
 */

@SideEffectFree
@Tags({"json", "json-validation", "schema-validation"})
@CapabilityDescription("Validate json using well defined schema")
public class JsonValidator extends AbstractProcessor {

    public static final String ERROR_ATTRIBUTE_KEY = "jsonvalidate.invalid.error";

    // relationships
    public static final Relationship REL_VALID = new Relationship.Builder()
            .name("valid")
            .description("Flowfiles that have been successfully validated are transferred to this relationship")
            .build();

    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description(
                    "Flowfiles with unsuccessful validation are transferred to this relationship")
            .build();

    public static final PropertyDescriptor JSON_SCHEMA = new PropertyDescriptor.Builder()
            .name("Json Schema")
            .description(
                    "Path of json schema to validate the flowfile with. To create a schema, check out: http://json-schema.org/examples.html")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(false)
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private final AtomicReference<Schema> schemaRef = new AtomicReference<Schema>();


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
        properties.add(JSON_SCHEMA);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_VALID);
        relationships.add(REL_INVALID);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final AtomicBoolean valid = new AtomicBoolean(true);
        final AtomicReference<Exception> exception = new AtomicReference<Exception>(null);
        final JSONObject jsonSchema = new JSONObject(context.getProperty(JSON_SCHEMA).getValue());
        final Schema schema = SchemaLoader.load(jsonSchema);
        session.read(flowFile, new InputStreamCallback() {
            public void process(InputStream inputStream) throws IOException {
                try{
                    String inputJson = IOUtils.toString(inputStream);
                    schema.validate(new JSONObject(inputJson));
                }catch (final IllegalArgumentException e){
                    valid.set(false);
                    exception.set(e);
                }catch (ValidationException e){
                    valid.set(false);
                    exception.set(e);
                }
            }
        });

        if (valid.get()) {
            logger.debug("Successfully validated {} against schema; routing to 'valid'", new Object[]{flowFile});
            session.getProvenanceReporter().route(flowFile, REL_VALID);
            session.transfer(flowFile, REL_VALID);
        } else {
            flowFile = session.putAttribute(flowFile, ERROR_ATTRIBUTE_KEY, exception.get().getLocalizedMessage());
            logger.info("Failed to validate {} against schema due to {}; routing to 'invalid'", new Object[]{flowFile, exception.get().getLocalizedMessage()});
            session.getProvenanceReporter().route(flowFile, REL_INVALID);
            session.transfer(flowFile, REL_INVALID);
        }


    }
}
