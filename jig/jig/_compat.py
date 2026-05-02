# Iron renamed rclpy.qos_event -> rclpy.event_handler. Humble only has the
# old name. Re-export from whichever exists so consumers can import here.
try:
    from rclpy.event_handler import (
        PublisherEventCallbacks,
        QoSLivelinessChangedInfo,
        QoSLivelinessLostInfo,
        QoSOfferedDeadlineMissedInfo,
        QoSRequestedDeadlineMissedInfo,
        SubscriptionEventCallbacks,
    )
except ImportError:
    from rclpy.qos_event import (
        PublisherEventCallbacks,
        QoSLivelinessChangedInfo,
        QoSLivelinessLostInfo,
        QoSOfferedDeadlineMissedInfo,
        QoSRequestedDeadlineMissedInfo,
        SubscriptionEventCallbacks,
    )

# Iron split ClockType out into rclpy.clock_type. Humble exposes it from
# rclpy.clock.
try:
    from rclpy.clock_type import ClockType
except ImportError:
    from rclpy.clock import ClockType


# Node.create_timer gained an `autostart` kwarg post-Humble (Jazzy+).
# On Humble the kwarg doesn't exist; create the timer and cancel() it manually
# to get the same effect. Mirrors JIG_HAS_TIMER_AUTOSTART in compat.hpp.
import inspect as _inspect

from rclpy.node import Node as _Node

HAS_TIMER_AUTOSTART = "autostart" in _inspect.signature(_Node.create_timer).parameters


__all__ = [
    "ClockType",
    "HAS_TIMER_AUTOSTART",
    "PublisherEventCallbacks",
    "QoSLivelinessChangedInfo",
    "QoSLivelinessLostInfo",
    "QoSOfferedDeadlineMissedInfo",
    "QoSRequestedDeadlineMissedInfo",
    "SubscriptionEventCallbacks",
]
