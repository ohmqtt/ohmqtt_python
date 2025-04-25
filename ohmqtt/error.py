from .mqtt_spec import MQTTReasonCodeReverse


class MQTTError(Exception):
    def __init__(self, message: str, reason_code: int):
        super().__init__(message)
        self.reason_code = reason_code

    def __str__(self) -> str:
        return f"{super().__str__()} (reason code: {MQTTReasonCodeReverse[self.reason_code]}{{{hex(self.reason_code)}}})"
