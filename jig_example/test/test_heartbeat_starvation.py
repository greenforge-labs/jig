"""The ~/state heartbeat must keep publishing while a transition callback blocks the main executor.

A lifecycle manager watches children via the heartbeat's QoS deadline; if a long
configure/activate silences the heartbeat, a parent manager would cascade down a healthy node.
During the blocked transition the heartbeat must repeat the last COMMITTED primary state.
"""

import time
import unittest

from helpers import TIMEOUT, state_qos, transition_node, wait_for_node_state
import launch
import launch_ros.actions
import launch_testing
import launch_testing.actions
import pytest
import rclpy

from lifecycle_msgs.msg import State, Transition

from example_interfaces.srv import Trigger
from lifecycle_msgs.srv import ChangeState

TRANSITION_DELAY_S = 2.0
OBSERVE_S = 1.5
# Allows one missed 100 ms heartbeat plus spin-cadence jitter; the downstream deadline is 200 ms.
MAX_GAP_S = 0.35
# Bounds service-dispatch latency so a delayed dispatch cannot fake a pass (the whole observation
# window must overlap the blocked transition).
MAX_DISPATCH_S = 0.5


def _make_slow_node(name, param, delay_s):
    return launch_ros.actions.Node(
        package="jig_example",
        executable="slow_node",
        name=name,
        namespace=name,
        output="screen",
        parameters=[{"autostart": False, param: int(delay_s * 1000)}],
    )


@pytest.mark.launch_test
def generate_test_description():
    return launch.LaunchDescription(
        [
            _make_slow_node("slow_activate", "activate_delay_ms", TRANSITION_DELAY_S),
            _make_slow_node("slow_configure", "configure_delay_ms", TRANSITION_DELAY_S),
            _make_slow_node("slow_shutdown", "shutdown_delay_ms", TRANSITION_DELAY_S),
            _make_slow_node("slow_steady", "block_main_ms", TRANSITION_DELAY_S),
            launch_testing.actions.ReadyToTest(),
        ]
    )


class TestHeartbeatDuringBlockedTransition(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        rclpy.init()
        cls.node = rclpy.create_node("test_heartbeat_starvation")

    @classmethod
    def tearDownClass(cls):
        cls.node.destroy_node()
        rclpy.shutdown()

    def _observe_blocked_transition(self, name, transition_id, expected_state_id):
        """Trigger a transition whose callback blocks for TRANSITION_DELAY_S and assert the
        heartbeat keeps publishing expected_state_id (the last committed primary state)."""
        received = []  # (monotonic receipt time, state id)
        sub = self.node.create_subscription(
            State,
            f"{name}/state",
            lambda msg: received.append((time.monotonic(), msg.id)),
            state_qos(),
        )

        # Barrier: the publisher is gated on subscription count, so wait for the DDS match and
        # first delivery before opening the timed window.
        end = time.monotonic() + TIMEOUT
        while not received and time.monotonic() < end:
            rclpy.spin_once(self.node, timeout_sec=0.05)
        self.assertTrue(received, "no heartbeat before transition — subscription never matched")

        client = self.node.create_client(ChangeState, f"{name}/change_state")
        self.assertTrue(client.wait_for_service(timeout_sec=TIMEOUT))
        future = client.call_async(ChangeState.Request(transition=Transition(id=transition_id)))

        start = time.monotonic()
        while time.monotonic() - start < OBSERVE_S:
            rclpy.spin_once(self.node, timeout_sec=0.05)

        in_window = [(t, sid) for (t, sid) in received if t - start >= 0.2 and t - start <= OBSERVE_S]
        self.assertGreaterEqual(
            len(in_window),
            5,
            f"heartbeat starved during blocked transition: {len(in_window)} samples",
        )
        for _, state_id in in_window:
            self.assertEqual(state_id, expected_state_id)

        # No silent gap anywhere from the last pre-window sample to the window end — mirrors the
        # downstream QoS-deadline detector rather than an aggregate count.
        stamps = [t for (t, _) in received if t <= start + OBSERVE_S]
        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        self.assertLess(max(gaps), MAX_GAP_S, f"heartbeat gap {max(gaps):.2f}s exceeds {MAX_GAP_S}s")

        rclpy.spin_until_future_complete(self.node, future, timeout_sec=TIMEOUT)
        result = future.result()
        self.assertIsNotNone(result)
        self.assertTrue(result.success)
        # Completion-time bound: proves the transition was dispatched within MAX_DISPATCH_S (and
        # therefore blocking for most of the window), so the gap assertion above genuinely
        # exercised the blocked region — late dispatch cannot fake a pass.
        self.assertLess(time.monotonic() - start, MAX_DISPATCH_S + TRANSITION_DELAY_S)

        self.node.destroy_client(client)
        self.node.destroy_subscription(sub)

    def test_heartbeat_continues_while_activate_blocks(self):
        name = "/slow_activate/slow_activate"
        self.assertTrue(transition_node(self.node, name, Transition.TRANSITION_CONFIGURE))
        self.assertTrue(wait_for_node_state(self.node, name, State.PRIMARY_STATE_INACTIVE))

        self._observe_blocked_transition(name, Transition.TRANSITION_ACTIVATE, State.PRIMARY_STATE_INACTIVE)
        self.assertTrue(wait_for_node_state(self.node, name, State.PRIMARY_STATE_ACTIVE))

    def test_heartbeat_reports_unconfigured_while_configure_blocks(self):
        name = "/slow_configure/slow_configure"
        self.assertTrue(
            wait_for_node_state(self.node, name, State.PRIMARY_STATE_UNCONFIGURED, timeout=5.0),
        )

        self._observe_blocked_transition(name, Transition.TRANSITION_CONFIGURE, State.PRIMARY_STATE_UNCONFIGURED)
        self.assertTrue(wait_for_node_state(self.node, name, State.PRIMARY_STATE_INACTIVE))

    def test_heartbeat_reports_active_while_shutdown_blocks(self):
        """A blocked on_shutdown must NOT report FINALIZED early — teardown is still running."""
        name = "/slow_shutdown/slow_shutdown"
        self.assertTrue(transition_node(self.node, name, Transition.TRANSITION_CONFIGURE))
        self.assertTrue(transition_node(self.node, name, Transition.TRANSITION_ACTIVATE))
        self.assertTrue(wait_for_node_state(self.node, name, State.PRIMARY_STATE_ACTIVE))

        self._observe_blocked_transition(name, Transition.TRANSITION_ACTIVE_SHUTDOWN, State.PRIMARY_STATE_ACTIVE)
        self.assertTrue(wait_for_node_state(self.node, name, State.PRIMARY_STATE_FINALIZED))

    def test_heartbeat_goes_silent_when_main_executor_wedges(self):
        """Outside transitions the heartbeat attests main-executor liveness: a node wedged in a
        user callback while ACTIVE must go silent so a parent's QoS-deadline watch can detect it."""
        name = "/slow_steady/slow_steady"
        self.assertTrue(transition_node(self.node, name, Transition.TRANSITION_CONFIGURE))
        self.assertTrue(transition_node(self.node, name, Transition.TRANSITION_ACTIVATE))
        self.assertTrue(wait_for_node_state(self.node, name, State.PRIMARY_STATE_ACTIVE))

        received = []
        sub = self.node.create_subscription(
            State,
            f"{name}/state",
            lambda msg: received.append(time.monotonic()),
            state_qos(),
        )
        end = time.monotonic() + TIMEOUT
        while not received and time.monotonic() < end:
            rclpy.spin_once(self.node, timeout_sec=0.05)
        self.assertTrue(received, "no heartbeat before wedge — subscription never matched")

        client = self.node.create_client(Trigger, f"{name}/block_main")
        self.assertTrue(client.wait_for_service(timeout_sec=TIMEOUT))
        future = client.call_async(Trigger.Request())

        start = time.monotonic()
        while time.monotonic() - start < OBSERVE_S:
            rclpy.spin_once(self.node, timeout_sec=0.05)

        silent_window = [t for t in received if t - start >= 0.3 and t - start <= OBSERVE_S]
        self.assertEqual(
            len(silent_window),
            0,
            f"heartbeat kept publishing while the main executor was wedged ({len(silent_window)} samples) — "
            "a parent manager could never detect this node",
        )

        rclpy.spin_until_future_complete(self.node, future, timeout_sec=TIMEOUT)
        result = future.result()
        self.assertIsNotNone(result)
        self.assertTrue(result.success)

        # Heartbeat resumes once the executor is free again.
        resume_count = len(received)
        end = time.monotonic() + 2.0
        while time.monotonic() < end:
            rclpy.spin_once(self.node, timeout_sec=0.05)
        self.assertGreater(len(received), resume_count, "heartbeat did not resume after the wedge cleared")

        self.node.destroy_client(client)
        self.node.destroy_subscription(sub)
