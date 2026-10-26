#include "readerwriterqueue.h"
#include <cstdint>
#include <memory>
#include <vector>

// The same queue is embedded in the VFS's heap-allocated database object.
// Keep allocations alive together to exercise different allocator addresses.
int main() {
    using Queue = moodycamel::ReaderWriterQueue<void *>;
    std::vector<std::unique_ptr<Queue>> queues;
    for (size_t i = 0; i < 64; ++i) {
        queues.emplace_back(new Queue(8));
        auto &queue = queues.back();
        if (reinterpret_cast<uintptr_t>(queue.get()) % alignof(Queue) != 0) {
            return 1;
        }
        void *value = nullptr;
        if (!queue->enqueue(queue.get()) || !queue->try_dequeue(value) || value != queue.get()) {
            return 2;
        }
    }
    return 0;
}
