#include "slow_node.hpp"

#include <chrono>
#include <memory>
#include <thread>

#include <example_interfaces/srv/trigger.hpp>

namespace jig_example::slow_node {

// Blocks transition callbacks (and, via ~/block_main, arbitrary main-executor work) on demand so
// tests can observe heartbeat behaviour while the main executor is busy.

void block_main_handler(
    std::shared_ptr<Session> sn,
    example_interfaces::srv::Trigger::Request::SharedPtr /*request*/,
    example_interfaces::srv::Trigger::Response::SharedPtr response
) {
    std::this_thread::sleep_for(std::chrono::milliseconds(sn->params.block_main_ms));
    response->success = true;
}

CallbackReturn on_configure(std::shared_ptr<Session> sn) {
    sn->services.block_main->set_request_handler(block_main_handler);
    std::this_thread::sleep_for(std::chrono::milliseconds(sn->params.configure_delay_ms));
    return CallbackReturn::SUCCESS;
}

CallbackReturn on_activate(std::shared_ptr<Session> sn) {
    std::this_thread::sleep_for(std::chrono::milliseconds(sn->params.activate_delay_ms));
    return CallbackReturn::SUCCESS;
}

CallbackReturn on_deactivate(std::shared_ptr<Session> /*sn*/) { return CallbackReturn::SUCCESS; }

CallbackReturn on_cleanup(std::shared_ptr<Session> /*sn*/) { return CallbackReturn::SUCCESS; }

void on_shutdown(std::shared_ptr<Session> sn) {
    std::this_thread::sleep_for(std::chrono::milliseconds(sn->params.shutdown_delay_ms));
}

} // namespace jig_example::slow_node
