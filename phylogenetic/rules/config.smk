
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


def conditional_config(option, *rule_parts):
    """
    Retrieves a wildcard-dependent config value and uses `conditional_arg` to decide
    whether to return an empty string or a string of "<option> <value>"

    The *rule_parts* arguments point to the config-defined value _within_ the relevant config block for this
    wildcard (i.e. within `config['build_params'][wildcards.build]`)
    """
    def _resolve(wildcards):
        try:
            config_block = config['build_params'][wildcards.build]
        except KeyError:
            raise WorkflowError(f"Failed to retrieve the config block for {wildcards.build=} when resolving the config path for {path=}")

        # now retrieve the actual value from nested dicts
        try:
            config_lookup = config_block
            for i,rule_key in enumerate(rule_parts[0:-1]):
                config_lookup = config_lookup[rule_key]
        except KeyError:
            raise WorkflowError(f"Config block for {wildcards.build=} missing entry for " + ''.join(['["'+rule_parts[j]+'"]' for j in range(0,i+1)]))
        if not isinstance(config_lookup, dict):
            raise WorkflowError(f"Config block for {wildcards.build=} for " + ''.join(['["'+rule_parts[j]+'"]' for j in range(0,i+1)]), " must be a dict")

        argument = config_lookup.get(rule_parts[-1], None)

        return conditional_arg(option, argument)
    return _resolve

def conditional_arg(option, argument):
    """
    Used for config-defined arguments whose presence necessitates a command-line option
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
        if len(argument)==0:
            return "" # empty list interpreted as no command line args (empty string)
        return [option, *argument]
    if isinstance(argument, int) or isinstance(argument, float) or isinstance(argument, str):
        return [option, argument]
    raise WorkflowError(f"Workflow function conditional_arg() received an argument value of unexpected type: {type(argument).__name__}")


include: "../../shared/vendored/snakemake/config.smk"

# Modify the vendored `resolve_config_path` to suite the config style of this repo
def config_path(*rule_parts):
    def _resolve(wildcards):
        try:
            config_block = config['build_params'][wildcards.build]
        except KeyError:
            raise WorkflowError(f"Failed to retrieve the config block for {wildcards.build=} when resolving the config path for {path=}")

        # now retrieve the actual value from nested dicts
        try:
            config_lookup = config_block
            for i,rule_key in enumerate(rule_parts):
                config_lookup = config_lookup[rule_key]
        except KeyError:
            raise WorkflowError(f"Config block for {wildcards.build=} missing entry for " + ''.join(['["'+rule_parts[j]+'"]' for j in range(0,i+1)]))

        return resolve_config_path(config_lookup, PHYLO_DIR)
    return _resolve