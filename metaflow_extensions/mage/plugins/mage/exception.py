"""Exceptions for the Mage Metaflow deployer plugin."""

from metaflow.exception import MetaflowException


class MageException(MetaflowException):
    """Raised when a Mage API call fails or returns an unexpected response."""

    headline = "Mage error"


class NotSupportedException(MetaflowException):
    """Raised when a Metaflow feature is not supported by the Mage deployer."""

    headline = "Not supported"
