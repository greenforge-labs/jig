#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <type_traits>

#include <lifecycle_msgs/msg/state.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp_lifecycle/lifecycle_node.hpp>

#include "compat.hpp"
#include "fixed_string.hpp"
#include "session.hpp"

namespace jig {

using CallbackReturn = rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn;

template <
    fixed_string node_name,
    typename SessionType,
    auto extend_options = [](rclcpp::NodeOptions options) { return options; }>
class BaseNode {
    static_assert(std::is_base_of_v<Session, SessionType>, "SessionType must derive from jig::Session");

  public:
    explicit BaseNode(const rclcpp::NodeOptions &options)
        : node_(std::make_shared<rclcpp_lifecycle::LifecycleNode>(
              node_name.c_str(), extend_options(rclcpp::NodeOptions(options).use_intra_process_comms(true))
          )) {
        client_cb_group_ = node_->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive, false);
        client_executor_ = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
        client_executor_->add_callback_group(client_cb_group_, node_->get_node_base_interface());
        client_executor_thread_ = std::thread([this]() { client_executor_->spin(); });

        node_->register_on_configure([this](const auto &) { return handle_configure(); });
        node_->register_on_activate([this](const auto &) { return handle_activate(); });
        node_->register_on_deactivate([this](const auto &) { return handle_deactivate(); });
        node_->register_on_cleanup([this](const auto &) { return handle_cleanup(); });
        node_->register_on_shutdown([this](const auto &) { return handle_shutdown(); });
        node_->register_on_error([this](const auto &) { return handle_error(); });

        // State heartbeat publisher + timer (always active, not lifecycle-managed).
        // JIG_INTRAPROCESS_DURABILITY: transient_local on Iron+, volatile on Humble (see compat.hpp).
        auto state_qos = rclcpp::QoS(1)
                             .reliable()
                             .JIG_INTRAPROCESS_DURABILITY.deadline(std::chrono::milliseconds(100))
                             .liveliness(rclcpp::LivelinessPolicy::Automatic)
                             .liveliness_lease_duration(std::chrono::milliseconds(100));
        auto node_params = node_->get_node_parameters_interface();
        auto node_topics = node_->get_node_topics_interface();
        state_pub_ =
            rclcpp::create_publisher<lifecycle_msgs::msg::State>(node_params, node_topics, "~/state", state_qos);
        // ~/state deliberately has two publish paths:
        //  - Steady state: this main-executor timer, so the heartbeat attests main-executor
        //    liveness — a node wedged in a user callback goes silent and a parent watching the
        //    QoS deadline detects it.
        //  - During a transition: the main executor is inside the transition callback, so a
        //    LEGITIMATELY long configure/activate would silence the heartbeat and get a healthy
        //    node cascaded. transition_heartbeat_thread_ (below) covers exactly that window,
        //    publishing the last committed primary state from reported_state_id_. A dedicated
        //    thread, not the client executor: codegen wires every service/action client into
        //    client_cb_group_, so user client callbacks could starve a heartbeat living there.
        state_timer_ = node_->create_wall_timer(std::chrono::milliseconds(100), [this]() {
            if (state_pub_->get_subscription_count() == 0) {
                return;
            }
            auto msg = std::make_unique<lifecycle_msgs::msg::State>();
            auto state = node_->get_current_state();
            msg->id = state.id();
            msg->label = state.label();
            state_pub_->publish(std::move(msg));
        });
        transition_heartbeat_thread_ = std::thread([this]() {
            while (heartbeat_running_.load(std::memory_order_relaxed)) {
                if (transitioning_.load(std::memory_order_relaxed) && state_pub_->get_subscription_count() > 0) {
                    auto msg = std::make_unique<lifecycle_msgs::msg::State>();
                    msg->id = reported_state_id_.load(std::memory_order_relaxed);
                    msg->label = primary_state_label(msg->id);
                    state_pub_->publish(std::move(msg));
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });

        node_->declare_parameter("autostart", true);
        if (node_->get_parameter("autostart").as_bool()) {
            using lifecycle_msgs::msg::State;
            autostart_timer_ = node_->create_wall_timer(std::chrono::seconds(0), [this]() {
                autostart_timer_->cancel();
                if (node_->configure().id() != State::PRIMARY_STATE_INACTIVE) {
                    RCLCPP_ERROR(node_->get_logger(), "Autostart failed to configure");
                    return;
                }
                if (node_->activate().id() != State::PRIMARY_STATE_ACTIVE) {
                    RCLCPP_ERROR(node_->get_logger(), "Autostart failed to activate");
                }
            });
        }
    }

    virtual ~BaseNode() {
        heartbeat_running_.store(false, std::memory_order_relaxed);
        if (transition_heartbeat_thread_.joinable()) {
            transition_heartbeat_thread_.join();
        }
        client_executor_->cancel();
        if (client_executor_thread_.joinable()) {
            client_executor_thread_.join();
        }
    }

    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr get_node_base_interface() const {
        return node_->get_node_base_interface();
    }

  private:
    // reported_state_id_ tracks committed primary states only: on SUCCESS the transition's target,
    // on FAILURE its origin (mirroring the lifecycle state machine); a CallbackReturn::ERROR leaves
    // it untouched — the machine routes through handle_error, which reports FINALIZED. During a
    // transition the heartbeat therefore repeats the last committed primary state, and on SUCCESS
    // it may lead the state machine's own commit by the bookkeeping interval.
    static const char *primary_state_label(std::uint8_t id) {
        using lifecycle_msgs::msg::State;
        switch (id) {
        case State::PRIMARY_STATE_UNCONFIGURED:
            return "unconfigured";
        case State::PRIMARY_STATE_INACTIVE:
            return "inactive";
        case State::PRIMARY_STATE_ACTIVE:
            return "active";
        case State::PRIMARY_STATE_FINALIZED:
            return "finalized";
        default:
            return "unknown";
        }
    }

    // Marks a transition in flight so the transition heartbeat thread covers the window where
    // the main executor is inside the (possibly long) transition callback.
    struct TransitionScope {
        explicit TransitionScope(std::atomic<bool> &flag) : flag_(flag) {
            flag_.store(true, std::memory_order_relaxed);
        }
        ~TransitionScope() { flag_.store(false, std::memory_order_relaxed); }
        std::atomic<bool> &flag_;
    };

    void report_state(CallbackReturn result, std::uint8_t on_success, std::uint8_t on_failure) {
        if (result == CallbackReturn::SUCCESS) {
            reported_state_id_.store(on_success, std::memory_order_relaxed);
        } else if (result == CallbackReturn::FAILURE) {
            reported_state_id_.store(on_failure, std::memory_order_relaxed);
        }
    }

    CallbackReturn handle_configure() {
        using lifecycle_msgs::msg::State;
        TransitionScope scope(transitioning_);
        session_ = create_session(*node_);
        auto result = user_on_configure(session_);
        if (result == CallbackReturn::FAILURE) {
            session_.reset();
        }
        report_state(result, State::PRIMARY_STATE_INACTIVE, State::PRIMARY_STATE_UNCONFIGURED);
        return result;
    }

    CallbackReturn handle_activate() {
        using lifecycle_msgs::msg::State;
        TransitionScope scope(transitioning_);
        auto result = user_on_activate(session_);
        if (result == CallbackReturn::SUCCESS) {
            activate_entities(session_);
        }
        report_state(result, State::PRIMARY_STATE_ACTIVE, State::PRIMARY_STATE_INACTIVE);
        return result;
    }

    CallbackReturn handle_deactivate() {
        using lifecycle_msgs::msg::State;
        TransitionScope scope(transitioning_);
        auto result = user_on_deactivate(session_);
        if (result == CallbackReturn::SUCCESS) {
            deactivate_entities(session_);
        }
        report_state(result, State::PRIMARY_STATE_INACTIVE, State::PRIMARY_STATE_ACTIVE);
        return result;
    }

    CallbackReturn handle_cleanup() {
        using lifecycle_msgs::msg::State;
        TransitionScope scope(transitioning_);
        auto result = user_on_cleanup(session_);
        if (result == CallbackReturn::SUCCESS) {
            session_.reset();
        }
        report_state(result, State::PRIMARY_STATE_UNCONFIGURED, State::PRIMARY_STATE_INACTIVE);
        return result;
    }

    CallbackReturn handle_shutdown() {
        using lifecycle_msgs::msg::State;
        TransitionScope scope(transitioning_);
        if (session_) {
            user_on_shutdown(session_);
            session_.reset();
        }
        // FINALIZED only after teardown completes — publishing it while user_on_shutdown is
        // still commanding hardware would falsely advertise teardown-complete for the whole hook.
        reported_state_id_.store(State::PRIMARY_STATE_FINALIZED, std::memory_order_relaxed);
        return CallbackReturn::SUCCESS;
    }

    CallbackReturn handle_error() {
        using lifecycle_msgs::msg::State;
        TransitionScope scope(transitioning_);
        session_.reset();
        reported_state_id_.store(State::PRIMARY_STATE_FINALIZED, std::memory_order_relaxed);
        // always return failure so that we end in Finalized (our assertion is errors are unrecoverable)
        return CallbackReturn::FAILURE;
    }

  protected:
    virtual std::shared_ptr<SessionType> create_session(rclcpp_lifecycle::LifecycleNode &node) = 0;
    virtual void activate_entities(std::shared_ptr<SessionType> /*sn*/) {}
    virtual void deactivate_entities(std::shared_ptr<SessionType> /*sn*/) {}

    virtual CallbackReturn user_on_configure(std::shared_ptr<SessionType> /*sn*/) { return CallbackReturn::SUCCESS; }
    virtual CallbackReturn user_on_activate(std::shared_ptr<SessionType> /*sn*/) { return CallbackReturn::SUCCESS; }
    virtual CallbackReturn user_on_deactivate(std::shared_ptr<SessionType> /*sn*/) { return CallbackReturn::SUCCESS; }
    virtual CallbackReturn user_on_cleanup(std::shared_ptr<SessionType> /*sn*/) { return CallbackReturn::SUCCESS; }
    virtual void user_on_shutdown(std::shared_ptr<SessionType> /*sn*/) {}

    rclcpp::CallbackGroup::SharedPtr client_callback_group() const { return client_cb_group_; }

    rclcpp_lifecycle::LifecycleNode::SharedPtr node_;
    std::shared_ptr<SessionType> session_;
    rclcpp::TimerBase::SharedPtr autostart_timer_;
    rclcpp::Publisher<lifecycle_msgs::msg::State>::SharedPtr state_pub_;
    rclcpp::TimerBase::SharedPtr state_timer_;
    std::atomic<std::uint8_t> reported_state_id_{lifecycle_msgs::msg::State::PRIMARY_STATE_UNCONFIGURED};
    std::atomic<bool> transitioning_{false};
    std::atomic<bool> heartbeat_running_{true};
    std::thread transition_heartbeat_thread_;
    rclcpp::CallbackGroup::SharedPtr client_cb_group_;
    std::shared_ptr<rclcpp::executors::SingleThreadedExecutor> client_executor_;
    std::thread client_executor_thread_;
};

} // namespace jig
