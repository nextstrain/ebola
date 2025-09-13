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
