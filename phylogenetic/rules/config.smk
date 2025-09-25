
# Load the default config YAML which must exist as a sister-file to the Snakefile
if not os.path.exists(os.path.join(workflow.basedir, 'config.yaml')):
    raise InvalidConfigError("No default config - this is the result of an error/bug in the underlying workflow.")
configfile: os.path.join(workflow.basedir, 'config.yaml')

# Merge in a config.yaml file if it exists in the current working directory
# (most often the directory where you ran `snakemake` from, but can be changed via `--directory`)
if os.path.exists("config.yaml"):
    configfile: "config.yaml"

# NOTE: Any extra configuration (--configfile, --config) will have been merged into the `config` structure
# such that the precedence of "configfile: < --configfile < --config" is maintained. This happens within
# `configfile` directives.


# ------------------------------------ TEMPORARY ------------------------------------
# We don't yet support multiple inputs, but we use the config interface which will support them
# so check here we aren't trying to use multiple inputs
if 'additional_inputs' in config or len(config['inputs'])!=1:
    print("This workflow is not yet set up for multiple inputs.")
    exit(1)


def conditional(option, argument):
    """Used for config-defined arguments whose presence necessitates a command-line option
    (e.g. --foo) prepended and whose absence should result in no option/arguments in the CLI command.
    *argument* can be falsey, in which case an empty string is returned (i.e. "don't pass anything
    to the CLI"), or a *list* or *string* or *number* in which case a flat list of options/args is returned,
    or *True* in which case a list of a single element (the option) is returned.
    Any other argument type is a WorkflowError
    """
    if not argument:
        return ""
    if argument is True: # must come before `isinstance(argument, int)` as bool is a subclass of int
        return [option]
    if isinstance(argument, list):
        return [option, *argument]
    if isinstance(argument, int) or isinstance(argument, float) or isinstance(argument, str):
        return [option, argument]
    raise WorkflowError(f"Workflow function conditional() received an argument value of unexpected type: {type(argument).__name__}")
