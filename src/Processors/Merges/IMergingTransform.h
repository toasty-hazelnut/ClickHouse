#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>

namespace DB
{

/// Base class for IMergingTransform.
/// It is needed to extract all non-template methods in single translation unit.  //?
class IMergingTransformBase : public IProcessor
{
public:
    IMergingTransformBase(
        size_t num_inputs,
        const Block & input_header,
        const Block & output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_);

    IMergingTransformBase(
        const Blocks & input_headers,
        const Block & output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_);

    // outputs为IProcessor中的成员 OutputPorts outputs;
    OutputPort & getOutputPort() { return outputs.front(); }

    /// Methods to add additional input port. It is possible to do only before the first call of `prepare`.
    void addInput();
    /// Need to be called after all inputs are added. (only if have_all_inputs was not specified).
    void setHaveAllInputs();

    Status prepare() override;

protected:
    virtual void onNewInput(); /// Is called when new input is added. Only if have_all_inputs = false.
    virtual void onFinish() {} /// Is called when all data is processed.

    /// Processor state.
    struct State
    {
        Chunk output_chunk;
        IMergingAlgorithm::Input input_chunk;

        bool has_input = false;
        bool is_finished = false;
        bool need_data = false;
        bool no_data = false;
        size_t next_input_to_read = 0;

        IMergingAlgorithm::Inputs init_chunks;
    };

    State state;

private:
    struct InputState
    {
        explicit InputState(InputPort & port_) : port(port_) {}

        InputPort & port;
        bool is_initialized = false;
    };

    std::vector<InputState> input_states;
    std::atomic<bool> have_all_inputs;
    bool is_initialized = false;
    UInt64 limit_hint = 0;
    bool always_read_till_end = false;

    IProcessor::Status prepareInitializeInputs();
};

/// Implementation of MergingTransform using IMergingAlgorithm.
template <typename Algorithm>
class IMergingTransform : public IMergingTransformBase
{
public:
    // MergingSortedTransform中 调用的是这个构造函数 （所以empty_chunk_on_finish的值应该是默认的false ）
    /* 猜测，MergingSortedTransform中  传给MergingSortedTransform中的后面的参数  header,
        num_inputs,
        description_,
        max_block_size_rows,
        max_block_size_bytes,
        sorting_queue_strategy,
        limit_,
        out_row_sources_buf_,
        use_average_block_sizes   
        对应  Args && ... args ？
        作为MergingSortedAlgorithm构造函数的参数？
    */
    template <typename ... Args>
    IMergingTransform(
        size_t num_inputs,
        const Block & input_header,
        const Block & output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,             // 0
        bool always_read_till_end_,
        Args && ... args)
        : IMergingTransformBase(num_inputs, input_header, output_header, have_all_inputs_, limit_hint_, always_read_till_end_)
        , algorithm(std::forward<Args>(args) ...)
    {
    }

    template <typename ... Args>
    IMergingTransform(
        const Blocks & input_headers,
        const Block & output_header,
        bool have_all_inputs_,
        UInt64 limit_hint_,
        bool always_read_till_end_,
        bool empty_chunk_on_finish_,
        Args && ... args)
        : IMergingTransformBase(input_headers, output_header, have_all_inputs_, limit_hint_, always_read_till_end_)
        , empty_chunk_on_finish(empty_chunk_on_finish_)
        , algorithm(std::forward<Args>(args) ...)
    {
    }

    // override IProcessor的work()
    // work调用后，谁来继续调其他的比如prepare?

    void work() override
    {
        if (!state.init_chunks.empty())
            // 见MergingSortedAlgorithm.cpp / ColumnGathererStream::initialize(
            algorithm.initialize(std::move(state.init_chunks));

        // init_chunks后，还会有state.has_input吗？ 猜测会，某个part的一个block处理完 要处理下一个block时
        /*
        state.has_input何时设置为true:
          prepare()中，if state.need_data -> if input.hasData() 
        */
        if (state.has_input)
        {
            // std::cerr << "Consume chunk with " << state.input_chunk.getNumRows()
            //           << " for input " << state.next_input_to_read << std::endl;
            // 
            // 从algorithm.consume的定义看，next_input_to_read 似乎是指从哪个part (input) 去读下一个block/chunk。 *当前*要读的，不是之后要读的
            algorithm.consume(state.input_chunk, state.next_input_to_read);
            state.has_input = false;
        }
        else if (state.no_data && empty_chunk_on_finish)
        {
            IMergingAlgorithm::Input current_input;
            algorithm.consume(current_input, state.next_input_to_read);
            state.no_data = false;
        }

        // merge
        // status -> state. status反映在state中
        IMergingAlgorithm::Status status = algorithm.merge();

        if ((status.chunk && status.chunk.hasRows()) || !status.chunk.getChunkInfos().empty())
        {
            // std::cerr << "Got chunk with " << status.chunk.getNumRows() << " rows" << std::endl;
            // state.output_chunk只在这里被设置
            state.output_chunk = std::move(status.chunk);
            // 设置为state.output_chunk=xx后，如果prepare再被调用，会output.push(this chunk);   暂不知道是否会返回Status::PortFull。。
        }

        if (status.required_source >= 0)
        {
            // std::cerr << "Required data for input " << status.required_source << std::endl;
            state.next_input_to_read = status.required_source;
            state.need_data = true;
            // 设置为state.need_data=true; 后，如果prepare再被调用，prepare可能会返回 Status::NeedData  于是其他processor会被调用？？
        }

        if (status.is_finished)
        {
            // std::cerr << "Finished" << std::endl;
            state.is_finished = true;
        }
    }

protected:
    /// Call `consume` with empty chunk when there is no more data.
    bool empty_chunk_on_finish = false;

    Algorithm algorithm;

    /// Profile info.
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

private:
    using IMergingTransformBase::state;
};

using MergingTransformPtr = std::shared_ptr<IMergingTransformBase>;

}
