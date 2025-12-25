class IncrementalDependencyError(Exception):
    """
    Raised when required records are missing across a table.
    """
    def __init__(self, message: str | None = None):
        super().__init__(message)
        