class ExitPipeline(Exception):
    """
    Exception to exit the pipeline
    """

    def __init__(self, message: str, error: False, *args):
        self.message = message
        super().__init__(*args)
