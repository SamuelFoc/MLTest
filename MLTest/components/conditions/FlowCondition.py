from MLTest.interfaces.Components import FlowComponent


class FlowCondition(FlowComponent):
    def __init__(self, check_function: callable, result_if_met: any = None, result_if_not_met: any = None):
        """
        Initializes the FlowCondition component.
        
        Parameters:
        - check_function (callable): A function that takes the output of the previous component and returns a boolean.
        - result_if_met (any): The value to return if the check condition is met.
        - result_if_not_met (any): The value to return if the check condition is not met.
        """
        self.check_function = check_function
        self.result_if_met = result_if_met
        self.result_if_not_met = result_if_not_met

    def use(self, previous_output: any) -> any:
        """
        Evaluates the condition on the output of the previous component and returns specified values.
        
        Parameters:
        - previous_output (any): The output of the previous component in the pipeline.
        
        Returns:
        - any: The value specified in `result_if_met` if the condition is met, otherwise `result_if_not_met`.
        """
        # Check the output using the provided check_function
        condition_met = self.check_function(previous_output)
        
        # Determine result based on whether the condition is met
        if condition_met:
            return self.result_if_met() if callable(self.result_if_met) else self.result_if_met
        else:
            return self.result_if_not_met() if callable(self.result_if_not_met) else self.result_if_not_met

