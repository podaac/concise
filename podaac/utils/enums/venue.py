from enum import Enum

class Venue(Enum):
    UAT = 1
    OPS = 2
    SIT = 3

    @staticmethod
    def from_str(label):
        if label.lower() in ["uat", "ngap_uat"]:
            return Venue.UAT
        elif label.lower() in ["ops", "ngap_ops", "prod"]:
            return Venue.OPS
        elif label.lower() in ["sit", "ngap_sit"]:
            return Venue.SIT
        else:
            raise NotImplemented(f'No matching set up for env value "{label}"!')