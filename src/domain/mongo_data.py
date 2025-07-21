
from datetime import date

from pandas.core.arrays.arrow import ListAccessor


class MongoData:
    """
    A class to represent MongoDB data.
    """

    def __init__(self, _id: str, data: list[dict]):
        """
        Initializes the MongoData instance.

        :param _id: The unique identifier for the document.
        :param data: The data contained in the document.
        """
        self._id = _id
        self.data = data

        self.created_at = date.today()

    def to_dict(self):
        """
        Converts the MongoData instance to a dictionary.

        :return: A dictionary representation of the MongoData instance.
        """
        return {
            '_id': self._id,
            'data': self.data,
            'created_at': self.created_at.isoformat()  # Convert date to ISO format
        }