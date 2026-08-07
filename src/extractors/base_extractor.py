from abc import ABC, abstractmethod

class BaseExtractor(ABC):
    """
    Abstract base class for all API extractors.
    """

    @abstractmethod
    def extract(self):
        pass