#pragma once

#include <Processors/Chunk.h>
#include <variant>

namespace DB
{

class IMergingAlgorithm
{
public:
    // Status中的Chunk是啥？ merge后得到的一个chunk
    struct Status
    {
        Chunk chunk;
        bool is_finished = false;
        
        // required_source似乎只会在algoirthm.merge中被设置。
        // 只会用于赋值给next_input_to_read, state.next_input_to_read = status.required_source; 只会在这里被用到
        ssize_t required_source = -1;

        explicit Status(Chunk chunk_) : chunk(std::move(chunk_)) {}   // 。。。。。。move
        explicit Status(Chunk chunk_, bool is_finished_) : chunk(std::move(chunk_)), is_finished(is_finished_) {}
        explicit Status(size_t source) : required_source(source) {}
    };

    struct Input
    {
        Chunk chunk;

        /// It is a flag which says that last row from chunk should be ignored in result.
        /// This row is not ignored in sorting and is needed to synchronize required source
        /// between different algorithm objects in parallel FINAL.
        bool skip_last_row = false;

        IColumn::Permutation * permutation = nullptr;

        void swap(Input & other) noexcept
        {
            chunk.swap(other.chunk);
            std::swap(skip_last_row, other.skip_last_row);
        }

        void set(Chunk chunk_)
        {
            chunk = std::move(chunk_);
            skip_last_row = false;
        }
    };

    using Inputs = std::vector<Input>;

    static void removeConstAndSparse(Input & input)
    {
        convertToFullIfConst(input.chunk);
        convertToFullIfSparse(input.chunk);
    }

    static void removeConstAndSparse(Inputs & inputs)
    {
        for (auto & input : inputs)
            removeConstAndSparse(input);
    }

    virtual const char * getName() const = 0;
    virtual void initialize(Inputs inputs) = 0;
    virtual void consume(Input & input, size_t source_num) = 0;
    virtual Status merge() = 0;

    IMergingAlgorithm() = default;
    virtual ~IMergingAlgorithm() = default;
};

// TODO: use when compile with clang which could support it
// template <class T>
// concept MergingAlgorithm = std::is_base_of<IMergingAlgorithm, T>::value;

}
