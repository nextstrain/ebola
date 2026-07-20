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

def validate_config():

    # HIGH-LEVEL STRUCTURE OF INPUTS / ADDITIONAL_INPUTS
    # Note: multiple_inputs.smk does further error checking
    all_inputs_all_species = [*config['inputs'], *config.get('additional_inputs', [])]    
    if not all([isinstance(i, dict) for i in all_inputs_all_species]):
        raise InvalidConfigError("All of the elements in config.inputs and config.additional_inputs lists must be dictionaries. "
            "If you've used a command line '--config' double check your quoting.")
    if not all(['species' in i for i in all_inputs_all_species]):
        raise InvalidConfigError("All of the elements in config.inputs and config.additional_inputs lists must have a 'species' key")
    
    # BUILDS
    if not isinstance(config.get('builds', False), list) or len(config['builds'])==0 or not all([isinstance(b, str) for b in config['builds']]):
        raise InvalidConfigError("config.builds must be a list with at least one value; values must be strings")
    if not all([len(b.split('/'))==2 for b in config['builds']]):
        raise InvalidConfigError("Each value in config.builds must have the format {species}/{build}, e.g. 'ebov/all-outbreaks'")

def write_subsample_configs():
    for build_pair in config["builds"]:
        species, build = build_pair.split('/')
        write_config(f"results/{species}/{build}/subsample_config.yaml", section=["subsample", build_pair])

