#include <memory>

#include <jig_example/slow_node_interface.hpp>

namespace jig_example::slow_node {

struct Session : SlowNodeSession<Session> {
    using SlowNodeSession::SlowNodeSession;
};

CallbackReturn on_configure(std::shared_ptr<Session> sn);
CallbackReturn on_activate(std::shared_ptr<Session> sn);
CallbackReturn on_deactivate(std::shared_ptr<Session> sn);
CallbackReturn on_cleanup(std::shared_ptr<Session> sn);
void on_shutdown(std::shared_ptr<Session> sn);

using SlowNode = SlowNodeBase<Session, on_configure, on_activate, on_deactivate, on_cleanup, on_shutdown>;

} // namespace jig_example::slow_node
