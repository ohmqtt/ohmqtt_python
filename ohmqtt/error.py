from .mqtt_spec import MQTTReasonCode


class MQTTError(Exception):
    def __init__(self, message: str, reason_code: MQTTReasonCode):
        super().__init__(message)
        self.reason_code = reason_code

    def __str__(self) -> str:
        return f"{super().__str__()} (reason code: {str(self.reason_code)})"
